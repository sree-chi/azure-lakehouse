# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver transform
# MAGIC Curates bronze into typed, clean tables:
# MAGIC * selects ~30 of the ~100 source columns, tolerating header drift between EPA file versions
# MAGIC * casts numbers safely (`try_cast`), turns YES/NO flags into booleans, blanks into NULL
# MAGIC * converts dioxin quantities from **grams to pounds** so every `*_lb` column is comparable
# MAGIC * de-duplicates to one row per TRI reporting form (`doc_ctrl_num`)
# MAGIC * records data-quality metrics for every run in `silver.dq_metrics`

# COMMAND ----------

dbutils.widgets.text("reporting_year", "2024")
dbutils.widgets.text("catalog", "tri_lakehouse")

reporting_year = int(dbutils.widgets.get("reporting_year"))
catalog = dbutils.widgets.get("catalog")

# COMMAND ----------

import os
import sys

nb_dir = os.path.dirname(
    "/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
)
if nb_dir not in sys.path:
    sys.path.insert(0, nb_dir)

from datetime import datetime, timezone

from pyspark.sql import functions as F
from tri_transforms import release_quality_metrics, standardize_facilities, standardize_releases

# COMMAND ----------

bronze_releases = spark.table(f"{catalog}.bronze.tri_releases").filter(
    F.col("reporting_year_partition") == reporting_year
)
silver_releases = standardize_releases(bronze_releases.drop("reporting_year_partition")).cache()

# Guard against a source file whose YEAR column disagrees with what we asked for.
wrong_year = silver_releases.filter(F.col("reporting_year") != reporting_year).count()
if wrong_year:
    raise ValueError(f"{wrong_year} rows have a reporting_year other than {reporting_year}; check the source URL.")

(
    silver_releases.write.format("delta")
    .mode("overwrite")
    .option("replaceWhere", f"reporting_year = {reporting_year}")
    .option("overwriteSchema", "false")
    .partitionBy("reporting_year")
    .saveAsTable(f"{catalog}.silver.tri_releases")
)

# COMMAND ----------

silver_facilities = standardize_facilities(spark.table(f"{catalog}.bronze.tri_facilities"))
(
    silver_facilities.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{catalog}.silver.tri_facilities")
)

# COMMAND ----------

# MAGIC %md ### Data-quality metrics

# COMMAND ----------

metrics = release_quality_metrics(bronze_releases.count(), silver_releases)
metrics["facility_registry_rows"] = silver_facilities.count()
run_ts = datetime.now(timezone.utc)

dq = spark.createDataFrame(
    [(run_ts, reporting_year, name, int(value)) for name, value in sorted(metrics.items())],
    "run_ts timestamp, reporting_year int, metric string, value long",
)
dq.write.format("delta").mode("append").saveAsTable(f"{catalog}.silver.dq_metrics")
display(dq)

# Fail the run (and therefore the ADF pipeline) if the data is clearly broken.
if metrics["silver_rows"] == 0:
    raise ValueError("Silver produced 0 release rows.")
if metrics["rows_unparseable_total"] > 0.05 * metrics["silver_rows"]:
    raise ValueError(f"More than 5% of TOTAL RELEASES values failed to parse: {metrics}")

silver_releases.unpersist()
