# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Load Azure SQL
# MAGIC Persists the gold tables to Azure SQL Database (the serving layer Power BI connects to),
# MAGIC then (re)creates the reporting views in `azure_sql_views.sql`.
# MAGIC
# MAGIC * Credentials come from the Key Vault-backed secret scope `kv`; nothing is hard-coded.
# MAGIC * `loginTimeout=90` because the free serverless database auto-pauses and takes ~1 minute to resume.

# COMMAND ----------

dbutils.widgets.text("catalog", "tri_lakehouse")
dbutils.widgets.text("sql_server_fqdn", "")
dbutils.widgets.text("sql_database", "tri_gold")

catalog = dbutils.widgets.get("catalog")
server = dbutils.widgets.get("sql_server_fqdn")
database = dbutils.widgets.get("sql_database")
assert server, "sql_server_fqdn job parameter is required"

user = dbutils.secrets.get("kv", "sql-admin-login")
password = dbutils.secrets.get("kv", "sql-admin-password")

jdbc_url = (
    f"jdbc:sqlserver://{server}:1433;database={database};"
    "encrypt=true;trustServerCertificate=false;"
    "hostNameInCertificate=*.database.windows.net;loginTimeout=90;"
)

# COMMAND ----------

GOLD_TABLES = {
    "facility_releases": "dbo.gold_facility_releases",
    "chemical_state_releases": "dbo.gold_chemical_state_releases",
    "parent_company_releases": "dbo.gold_parent_company_releases",
    "data_quality": "dbo.gold_data_quality",
}

for gold_name, sql_table in GOLD_TABLES.items():
    df = spark.table(f"{catalog}.gold.{gold_name}")
    (
        df.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", sql_table)
        .option("user", user)
        .option("password", password)
        .option("batchsize", 10000)
        # Full refresh: drop + recreate so schema changes in gold flow through.
        # Views are recreated right after, below.
        .mode("overwrite")
        .save()
    )
    print(f"{sql_table}: {df.count():,} rows")

# COMMAND ----------

# MAGIC %md ### Reporting views (idempotent `CREATE OR ALTER`)

# COMMAND ----------

import os
import re

nb_dir = os.path.dirname(
    "/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
with open(os.path.join(nb_dir, "azure_sql_views.sql"), encoding="utf-8") as f:
    # Split on lines that are exactly "GO" (sqlcmd batch separator).
    batches = [b.strip() for b in re.split(r"(?im)^\s*GO\s*$", f.read()) if b.strip()]
    # Drop comment-only batches.
    batches = [b for b in batches if any(l.strip() and not l.strip().startswith("--") for l in b.splitlines())]

conn = spark._jvm.java.sql.DriverManager.getConnection(jdbc_url, user, password)
try:
    stmt = conn.createStatement()
    for batch in batches:
        stmt.execute(batch)
    stmt.close()
finally:
    conn.close()

print(f"Applied {len(batches)} view definitions")
