# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Load Snowflake
# MAGIC Publishes the same gold tables to Snowflake (`TRI_DB.GOLD`), so the curated data is
# MAGIC available to a second warehouse without re-running the pipeline.
# MAGIC
# MAGIC * Uses the Snowflake Spark connector bundled with Databricks Runtime.
# MAGIC * Authenticates as a `TYPE = SERVICE` user with **key-pair auth**; Snowflake no longer allows
# MAGIC   password-only sign-in for service users. The private key lives only in Key Vault.
# MAGIC * Skipped when the job parameter `load_snowflake` is not `true`.

# COMMAND ----------

dbutils.widgets.text("catalog", "tri_lakehouse")
dbutils.widgets.text("load_snowflake", "false")
dbutils.widgets.text("snowflake_database", "TRI_DB")
dbutils.widgets.text("snowflake_schema", "GOLD")
dbutils.widgets.text("snowflake_warehouse", "TRI_WH")
dbutils.widgets.text("snowflake_role", "TRI_LOADER")

if dbutils.widgets.get("load_snowflake").strip().lower() != "true":
    dbutils.notebook.exit("Snowflake load skipped (load_snowflake != true)")

catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import re

pem = dbutils.secrets.get("kv", "snowflake-private-key")
# The connector wants the bare base64 body of an unencrypted PKCS#8 key.
pem_body = re.sub(r"-----(BEGIN|END) [A-Z ]+-----|\s", "", pem)

sf_options = {
    "sfURL": dbutils.secrets.get("kv", "snowflake-account-url"),  # e.g. myorg-myaccount.snowflakecomputing.com
    "sfUser": dbutils.secrets.get("kv", "snowflake-user"),
    "pem_private_key": pem_body,
    "sfDatabase": dbutils.widgets.get("snowflake_database"),
    "sfSchema": dbutils.widgets.get("snowflake_schema"),
    "sfWarehouse": dbutils.widgets.get("snowflake_warehouse"),
    "sfRole": dbutils.widgets.get("snowflake_role"),
}

# COMMAND ----------

GOLD_TABLES = {
    "facility_releases": "FACILITY_RELEASES",
    "chemical_state_releases": "CHEMICAL_STATE_RELEASES",
    "parent_company_releases": "PARENT_COMPANY_RELEASES",
    "data_quality": "DATA_QUALITY",
}

for gold_name, sf_table in GOLD_TABLES.items():
    df = spark.table(f"{catalog}.gold.{gold_name}")
    (
        df.write.format("snowflake")
        .options(**sf_options)
        .option("dbtable", sf_table)
        .mode("overwrite")
        .save()
    )
    print(f"{sf_options['sfDatabase']}.{sf_options['sfSchema']}.{sf_table}: {df.count():,} rows")
