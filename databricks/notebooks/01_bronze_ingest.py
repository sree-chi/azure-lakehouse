# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Bronze ingest
# MAGIC Reads the raw files Azure Data Factory landed in ADLS (`raw` container) and writes them,
# MAGIC unchanged except for Delta-safe column names, to `bronze` Delta tables.
# MAGIC
# MAGIC | Source | Format | Landed by ADF at | Bronze table |
# MAGIC |---|---|---|---|
# MAGIC | EPA TRI Basic Data File | CSV | `tri/releases/reporting_year=YYYY/ingest_date=YYYY-MM-DD/` | `bronze.tri_releases` |
# MAGIC | EPA Envirofacts TRI facility registry | JSON | `tri/facilities/ingest_date=YYYY-MM-DD/` | `bronze.tri_facilities` |
# MAGIC
# MAGIC Re-running a year replaces only that year's partition (`replaceWhere`), so the job is idempotent.

# COMMAND ----------

dbutils.widgets.text("reporting_year", "2024")
dbutils.widgets.text("ingest_date", "")  # blank = newest folder ADF landed
dbutils.widgets.text("catalog", "tri_lakehouse")
dbutils.widgets.text("storage_account", "")

reporting_year = int(dbutils.widgets.get("reporting_year"))
ingest_date = dbutils.widgets.get("ingest_date").strip()
catalog = dbutils.widgets.get("catalog")
storage_account = dbutils.widgets.get("storage_account")
assert storage_account, "storage_account job parameter is required"

raw_root = f"abfss://raw@{storage_account}.dfs.core.windows.net/tri"

# COMMAND ----------

import os
import sys

nb_dir = os.path.dirname(
    "/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
if nb_dir not in sys.path:
    sys.path.insert(0, nb_dir)

from pyspark.sql import functions as F
from tri_transforms import flatten_json_records, normalize_columns

# COMMAND ----------


def latest_ingest_folder(parent: str) -> str:
    """Pick the newest ingest_date=YYYY-MM-DD folder under `parent`."""
    folders = [f.name.rstrip("/") for f in dbutils.fs.ls(parent) if f.name.startswith("ingest_date=")]
    if not folders:
        raise FileNotFoundError(f"No ingest_date=... folders under {parent}. Has ADF run yet?")
    return sorted(folders)[-1].split("=", 1)[1]


releases_parent = f"{raw_root}/releases/reporting_year={reporting_year}"
facilities_parent = f"{raw_root}/facilities"

releases_date = ingest_date or latest_ingest_folder(releases_parent)
facilities_date = ingest_date or latest_ingest_folder(facilities_parent)
releases_path = f"{releases_parent}/ingest_date={releases_date}/"
facilities_path = f"{facilities_parent}/ingest_date={facilities_date}/"
print("releases  :", releases_path)
print("facilities:", facilities_path)

# COMMAND ----------

# MAGIC %md ### Releases (CSV → bronze.tri_releases)

# COMMAND ----------

raw_releases = (
    spark.read.option("header", True)
    .option("inferSchema", False)  # bronze keeps everything as string; typing happens in silver
    .option("escape", '"')
    .option("mode", "PERMISSIVE")
    .csv(releases_path)
    .select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
        F.lit(releases_date).alias("_ingest_date"),
    )
)

bronze_releases = normalize_columns(raw_releases).withColumn(
    "reporting_year_partition", F.lit(reporting_year)
)

(
    bronze_releases.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"reporting_year_partition = {reporting_year}")
    .option("mergeSchema", "true")
    .partitionBy("reporting_year_partition")
    .saveAsTable(f"{catalog}.bronze.tri_releases")
)

print(f"bronze.tri_releases: {bronze_releases.count():,} rows for {reporting_year}")

# COMMAND ----------

# MAGIC %md ### Facility registry (JSON → bronze.tri_facilities)
# MAGIC A full snapshot each run, so the table is simply overwritten.

# COMMAND ----------

raw_facilities = (
    spark.read.option("multiLine", True)
    .json(facilities_path)
)
raw_facilities = flatten_json_records(raw_facilities).select(
    "*",
    F.current_timestamp().alias("_ingested_at"),
    F.lit(facilities_date).alias("_ingest_date"),
)
bronze_facilities = normalize_columns(raw_facilities)

(
    bronze_facilities.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.bronze.tri_facilities")
)

print(f"bronze.tri_facilities: {bronze_facilities.count():,} rows")
