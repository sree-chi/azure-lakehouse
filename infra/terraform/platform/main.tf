# -----------------------------------------------------------------------------
# Platform stage: everything Azure-side that the pipeline runs on.
# Terraform owns the long-lived platform; Bicep (infra/bicep) owns the
# pipeline-layer artifacts (ADF pipeline + SQL database) that change more often.
# -----------------------------------------------------------------------------

data "azurerm_client_config" "current" {}

# The "AzureDatabricks" first-party app. Same app ID in every tenant.
# It needs read access to Key Vault for the Key Vault-backed secret scope.
data "azuread_service_principal" "azure_databricks" {
  client_id = "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d"
}

resource "random_string" "suffix" {
  length  = 5
  upper   = false
  special = false
}

locals {
  name = "${var.prefix}${random_string.suffix.result}"
  tags = {
    project    = "tri-lakehouse"
    managed_by = "terraform"
  }
}

resource "azurerm_resource_group" "rg" {
  name     = "rg-${local.name}"
  location = var.location
  tags     = local.tags
}

# -----------------------------------------------------------------------------
# Storage: ADLS Gen2 (hierarchical namespace)
#   raw/        landing zone that ADF writes source files into
#   lakehouse/  Unity Catalog managed storage for bronze/silver/gold Delta tables
# -----------------------------------------------------------------------------
resource "azurerm_storage_account" "lake" {
  name                            = "st${local.name}"
  resource_group_name             = azurerm_resource_group.rg.name
  location                        = azurerm_resource_group.rg.location
  account_tier                    = "Standard"
  account_replication_type        = "LRS"
  account_kind                    = "StorageV2"
  is_hns_enabled                  = true
  min_tls_version                 = "TLS1_2"
  allow_nested_items_to_be_public = false
  tags                            = local.tags
}

resource "azurerm_storage_container" "raw" {
  name                  = "raw"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

resource "azurerm_storage_container" "lakehouse" {
  name                  = "lakehouse"
  storage_account_id    = azurerm_storage_account.lake.id
  container_access_type = "private"
}

# -----------------------------------------------------------------------------
# Key Vault (RBAC mode): holds the SQL admin credentials and Snowflake key pair.
# Databricks reads it through a Key Vault-backed secret scope.
# -----------------------------------------------------------------------------
resource "azurerm_key_vault" "kv" {
  name                       = "kv-${local.name}"
  resource_group_name        = azurerm_resource_group.rg.name
  location                   = azurerm_resource_group.rg.location
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  enable_rbac_authorization  = true
  purge_protection_enabled   = false
  soft_delete_retention_days = 7
  tags                       = local.tags
}

# You (the deployer) can write secrets.
resource "azurerm_role_assignment" "me_kv_secrets_officer" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets Officer"
  principal_id         = data.azurerm_client_config.current.object_id
}

# Databricks control plane can read secrets (required for the KV-backed scope).
resource "azurerm_role_assignment" "databricks_kv_secrets_user" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = data.azuread_service_principal.azure_databricks.object_id
}

# RBAC assignments take a minute to propagate before the data plane honors them.
resource "time_sleep" "wait_for_kv_rbac" {
  depends_on      = [azurerm_role_assignment.me_kv_secrets_officer]
  create_duration = "90s"
}

# -----------------------------------------------------------------------------
# Azure SQL logical server (the database itself is created by Bicep).
# Entra ID admin = you, so Power BI and sqlcmd can sign in with your account.
# A SQL login also exists for the Databricks JDBC writer; its password lives
# only in Key Vault.
# -----------------------------------------------------------------------------
resource "random_password" "sql_admin" {
  length           = 24
  special          = true
  override_special = "!#%*-_=+"
  min_upper        = 2
  min_lower        = 2
  min_numeric      = 2
  min_special      = 2
}

resource "azurerm_mssql_server" "sql" {
  name                          = "sql-${local.name}"
  resource_group_name           = azurerm_resource_group.rg.name
  location                      = azurerm_resource_group.rg.location
  version                       = "12.0"
  administrator_login           = "triadmin"
  administrator_login_password  = random_password.sql_admin.result
  minimum_tls_version           = "1.2"
  public_network_access_enabled = true

  azuread_administrator {
    login_username = "tri-entra-admin"
    object_id      = data.azurerm_client_config.current.object_id
    tenant_id      = data.azurerm_client_config.current.tenant_id
  }

  tags = local.tags
}

# Databricks classic compute has no fixed egress IP without VNet injection,
# so allow Azure-hosted services. Production would use VNet injection + Private Endpoint.
resource "azurerm_mssql_firewall_rule" "allow_azure_services" {
  name             = "AllowAzureServices"
  server_id        = azurerm_mssql_server.sql.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

resource "azurerm_mssql_firewall_rule" "client" {
  count            = var.client_ip == "" ? 0 : 1
  name             = "ClientIp"
  server_id        = azurerm_mssql_server.sql.id
  start_ip_address = var.client_ip
  end_ip_address   = var.client_ip
}

resource "azurerm_key_vault_secret" "sql_admin_login" {
  name         = "sql-admin-login"
  value        = azurerm_mssql_server.sql.administrator_login
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [time_sleep.wait_for_kv_rbac]
}

resource "azurerm_key_vault_secret" "sql_admin_password" {
  name         = "sql-admin-password"
  value        = random_password.sql_admin.result
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [time_sleep.wait_for_kv_rbac]
}

# -----------------------------------------------------------------------------
# Azure Data Factory with a system-assigned managed identity.
# The identity is the only credential ADF uses: storage via RBAC, Databricks
# via a workspace service principal that can only run one job.
# -----------------------------------------------------------------------------
resource "azurerm_data_factory" "adf" {
  name                = "adf-${local.name}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

data "azuread_service_principal" "adf" {
  object_id = azurerm_data_factory.adf.identity[0].principal_id
}

resource "azurerm_role_assignment" "adf_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

# -----------------------------------------------------------------------------
# Azure Databricks workspace (Unity Catalog is auto-enabled on new workspaces)
# plus an Access Connector: the managed identity Unity Catalog uses to read
# and write ADLS, so no storage keys are ever handed to Databricks.
# -----------------------------------------------------------------------------
resource "azurerm_databricks_workspace" "dbw" {
  name                        = "dbw-${local.name}"
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = azurerm_resource_group.rg.location
  sku                         = var.databricks_sku
  managed_resource_group_name = "rg-${local.name}-dbw-managed"
  tags                        = local.tags
}

resource "azurerm_databricks_access_connector" "uc" {
  name                = "dbac-${local.name}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

resource "azurerm_role_assignment" "uc_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.uc.identity[0].principal_id
}

# You can browse raw files and Delta folders in the portal / Storage Explorer.
resource "azurerm_role_assignment" "me_storage" {
  scope                = azurerm_storage_account.lake.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = data.azurerm_client_config.current.object_id
}

# -----------------------------------------------------------------------------
# Cost guardrail: email alerts as spend approaches the monthly budget.
# -----------------------------------------------------------------------------
resource "azurerm_consumption_budget_resource_group" "budget" {
  count             = var.alert_email == "" ? 0 : 1
  name              = "budget-${local.name}"
  resource_group_id = azurerm_resource_group.rg.id
  amount            = var.budget_amount
  time_grain        = "Monthly"

  time_period {
    start_date = formatdate("YYYY-MM-01'T'00:00:00Z", timestamp())
  }

  dynamic "notification" {
    for_each = [50, 80, 100]
    content {
      enabled        = true
      threshold      = notification.value
      operator       = "GreaterThan"
      threshold_type = "Actual"
      contact_emails = [var.alert_email]
    }
  }

  lifecycle {
    ignore_changes = [time_period]
  }
}
