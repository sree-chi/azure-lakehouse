# -----------------------------------------------------------------------------
# Databricks stage: Unity Catalog storage, secrets, code, and the job.
# -----------------------------------------------------------------------------

data "databricks_current_user" "me" {}

data "databricks_spark_version" "lts" {
  long_term_support = true
  scala             = "2.13"
}

locals {
  code_dir   = "${path.module}/../../../databricks/notebooks"
  ws_folder  = "${data.databricks_current_user.me.home}/tri-lakehouse"
  abfss_raw  = "abfss://raw@${var.storage_account_name}.dfs.core.windows.net/"
  abfss_lake = "abfss://lakehouse@${var.storage_account_name}.dfs.core.windows.net/"
}

# -----------------------------------------------------------------------------
# Unity Catalog: the Access Connector's managed identity is the only way
# Databricks touches ADLS. No storage keys, no service principal secrets.
# -----------------------------------------------------------------------------
resource "databricks_storage_credential" "lake" {
  name    = "${var.storage_account_name}-mi"
  comment = "Access Connector managed identity for the TRI lakehouse storage account"

  azure_managed_identity {
    access_connector_id = var.access_connector_id
  }
}

resource "databricks_external_location" "raw" {
  name            = "${var.storage_account_name}-raw"
  url             = local.abfss_raw
  credential_name = databricks_storage_credential.lake.name
  comment         = "Landing zone written by Azure Data Factory"
}

resource "databricks_external_location" "lakehouse" {
  name            = "${var.storage_account_name}-lakehouse"
  url             = local.abfss_lake
  credential_name = databricks_storage_credential.lake.name
  comment         = "Managed storage for the TRI catalog"
}

resource "databricks_catalog" "tri" {
  name          = var.catalog_name
  storage_root  = "${local.abfss_lake}managed"
  comment       = "EPA Toxics Release Inventory lakehouse (bronze/silver/gold)"
  force_destroy = true
  depends_on    = [databricks_external_location.lakehouse]
}

resource "databricks_schema" "layer" {
  for_each      = toset(["bronze", "silver", "gold"])
  catalog_name  = databricks_catalog.tri.name
  name          = each.key
  force_destroy = true
}

# -----------------------------------------------------------------------------
# Secrets: a scope backed by Azure Key Vault. Notebooks call
# dbutils.secrets.get("kv", "<name>"); values never live in Databricks.
# -----------------------------------------------------------------------------
resource "databricks_secret_scope" "kv" {
  name = "kv"

  keyvault_metadata {
    resource_id = var.key_vault_id
    dns_name    = var.key_vault_uri
  }
}

# -----------------------------------------------------------------------------
# Code: notebooks + a shared Python module + the SQL view definitions.
# -----------------------------------------------------------------------------
resource "databricks_directory" "code" {
  path = local.ws_folder
}

locals {
  notebooks = {
    bronze    = { file = "01_bronze_ingest.py", language = "PYTHON" }
    silver    = { file = "02_silver_transform.py", language = "PYTHON" }
    gold      = { file = "03_gold_aggregate.scala", language = "SCALA" }
    sql       = { file = "04_load_azure_sql.py", language = "PYTHON" }
    snowflake = { file = "05_load_snowflake.py", language = "PYTHON" }
  }
  workspace_files = ["tri_transforms.py", "azure_sql_views.sql"]
}

resource "databricks_notebook" "nb" {
  for_each = local.notebooks
  path     = "${local.ws_folder}/${trimsuffix(trimsuffix(each.value.file, ".py"), ".scala")}"
  language = each.value.language
  source   = "${local.code_dir}/${each.value.file}"

  depends_on = [databricks_directory.code]
}

resource "databricks_workspace_file" "file" {
  for_each = toset(local.workspace_files)
  path     = "${local.ws_folder}/${each.key}"
  source   = "${local.code_dir}/${each.key}"

  depends_on = [databricks_directory.code]
}

# -----------------------------------------------------------------------------
# The job: five tasks on one shared single-node job cluster.
#   bronze_ingest -> silver_transform -> gold_aggregate -> load_azure_sql
#                                                      \-> load_snowflake
# -----------------------------------------------------------------------------
resource "databricks_job" "tri" {
  name                = "tri-lakehouse-pipeline"
  max_concurrent_runs = 1
  timeout_seconds     = 5400

  job_cluster {
    job_cluster_key = "single_node"

    new_cluster {
      spark_version      = data.databricks_spark_version.lts.id
      node_type_id       = var.node_type_id
      num_workers        = 0
      data_security_mode = "SINGLE_USER"
      single_user_name   = data.databricks_current_user.me.user_name

      spark_conf = {
        "spark.databricks.cluster.profile" = "singleNode"
        "spark.master"                     = "local[*, 4]"
      }

      custom_tags = {
        "ResourceClass" = "SingleNode"
        "project"       = "tri-lakehouse"
      }

      azure_attributes {
        availability = "ON_DEMAND_AZURE"
      }
    }
  }

  parameter {
    name    = "reporting_year"
    default = var.reporting_year
  }
  parameter {
    name    = "ingest_date"
    default = ""
  }
  parameter {
    name    = "catalog"
    default = databricks_catalog.tri.name
  }
  parameter {
    name    = "storage_account"
    default = var.storage_account_name
  }
  parameter {
    name    = "sql_server_fqdn"
    default = var.sql_server_fqdn
  }
  parameter {
    name    = "sql_database"
    default = var.sql_database_name
  }
  parameter {
    name    = "load_snowflake"
    default = var.enable_snowflake ? "true" : "false"
  }

  task {
    task_key        = "bronze_ingest"
    job_cluster_key = "single_node"
    notebook_task {
      notebook_path = databricks_notebook.nb["bronze"].path
    }
  }

  task {
    task_key        = "silver_transform"
    job_cluster_key = "single_node"
    depends_on {
      task_key = "bronze_ingest"
    }
    notebook_task {
      notebook_path = databricks_notebook.nb["silver"].path
    }
  }

  task {
    task_key        = "gold_aggregate"
    job_cluster_key = "single_node"
    depends_on {
      task_key = "silver_transform"
    }
    notebook_task {
      notebook_path = databricks_notebook.nb["gold"].path
    }
  }

  task {
    task_key        = "load_azure_sql"
    job_cluster_key = "single_node"
    depends_on {
      task_key = "gold_aggregate"
    }
    notebook_task {
      notebook_path = databricks_notebook.nb["sql"].path
    }
  }

  task {
    task_key        = "load_snowflake"
    job_cluster_key = "single_node"
    depends_on {
      task_key = "gold_aggregate"
    }
    notebook_task {
      notebook_path = databricks_notebook.nb["snowflake"].path
    }
  }

  email_notifications {
    on_failure = [data.databricks_current_user.me.user_name]
  }

  depends_on = [
    databricks_schema.layer,
    databricks_secret_scope.kv,
    databricks_workspace_file.file,
  ]
}

# -----------------------------------------------------------------------------
# Least privilege for the orchestrator: ADF's managed identity is registered
# as a workspace service principal that may only run (not edit) this one job.
# -----------------------------------------------------------------------------
resource "databricks_service_principal" "adf" {
  application_id   = var.adf_identity_app_id
  display_name     = "adf-managed-identity"
  workspace_access = true
}

resource "databricks_permissions" "job" {
  job_id = databricks_job.tri.id

  access_control {
    service_principal_name = databricks_service_principal.adf.application_id
    permission_level       = "CAN_MANAGE_RUN"
  }
}
