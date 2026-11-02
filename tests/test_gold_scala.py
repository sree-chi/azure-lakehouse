"""Runs the Scala GoldTransforms object from 03_gold_aggregate.scala in a local spark-shell.

1. Builds silver tables from the fixtures with the real Python transforms.
2. Extracts the code between `// BEGIN GoldTransforms` and `// END GoldTransforms`.
3. Runs it in spark-shell against that silver data and checks the results.

Skipped automatically when spark-shell isn't available.
"""

import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest
from pyspark.sql import functions as F

from tri_transforms import (
    flatten_json_records,
    normalize_columns,
    standardize_facilities,
    standardize_releases,
)

ROOT = Path(__file__).resolve().parents[1]
NOTEBOOK = ROOT / "databricks" / "notebooks" / "03_gold_aggregate.scala"


def _spark_shell() -> str | None:
    import pyspark

    candidate = Path(pyspark.__file__).parent / "bin" / "spark-shell"
    if candidate.exists():
        return str(candidate)
    return shutil.which("spark-shell")


def _gold_source() -> str:
    text = NOTEBOOK.read_text()
    m = re.search(r"// BEGIN GoldTransforms\n(.*?)// END GoldTransforms", text, re.S)
    assert m, "GoldTransforms markers not found in notebook"
    return m.group(1)


@pytest.mark.skipif(_spark_shell() is None, reason="spark-shell not available")
def test_gold_transforms_in_spark_shell(spark, fixtures_dir, tmp_path):
    raw = (
        spark.read.option("header", True)
        .option("escape", '"')
        .csv(str(fixtures_dir / "sample_releases.csv"))
        .select("*", F.current_timestamp().alias("_ingested_at"))
    )
    silver = standardize_releases(normalize_columns(raw))
    fac = standardize_facilities(
        normalize_columns(
            flatten_json_records(
                spark.read.option("multiLine", True).json(str(fixtures_dir / "sample_facilities.json"))
            )
        )
    )
    rel_path, fac_path = tmp_path / "rel", tmp_path / "fac"
    silver.write.parquet(str(rel_path))
    fac.write.parquet(str(fac_path))

    script = tmp_path / "gold_test.scala"
    script.write_text(
        _gold_source()
        + f'''
val rel = spark.read.parquet("{rel_path}")
val fac = spark.read.parquet("{fac_path}")

val f  = GoldTransforms.facilityReleases(rel)
val cs = GoldTransforms.chemicalStateReleases(rel)
val p  = GoldTransforms.parentCompanyReleases(rel)
val dq = GoldTransforms.dataQuality(rel, fac)

val dow = f.filter(F.col("trifd") === "48640THDWCBLDG4").head()
assert(dow.getAs[Long]("chemicals_reported") == 2L, "Dow chemicals")
assert(math.abs(dow.getAs[Double]("total_releases_lb") - 4210.504) < 0.01, "Dow total lb")
assert(math.abs(dow.getAs[Double]("carcinogen_releases_lb") - 0.004) < 0.001, "Dow carcinogen lb")

val topParent = p.filter(F.col("rank_in_year") === 1).head()
assert(topParent.getAs[String]("parent_company_label") == "DOW INC", "top parent")

val indep = p.filter(F.col("parent_company_label").endsWith("(independent)")).count()
assert(indep == 1L, "independent facility label")

assert(cs.filter(F.col("is_pfas") === true).count() == 1L, "pfas chemical rows")

val q = dq.head()
assert(q.getAs[Long]("release_rows") == 4L, "dq release rows")
assert(q.getAs[Long]("facilities_reporting") == 3L, "dq facilities reporting")
assert(q.getAs[Long]("facilities_in_registry") == 2L, "dq matched")
assert(math.abs(q.getAs[Double]("registry_match_rate") - 0.6667) < 0.0001, "dq rate")

println("GOLD_SCALA_TESTS_PASSED")
System.exit(0)
'''
    )
    env = dict(os.environ, SPARK_LOCAL_IP="127.0.0.1")
    proc = subprocess.run(
        [_spark_shell(), "--master", "local[2]", "--conf", "spark.ui.enabled=false"],
        input=f":load {script}\n:quit\n",
        capture_output=True,
        text=True,
        timeout=600,
        env=env,
    )
    out = proc.stdout + proc.stderr
    assert "GOLD_SCALA_TESTS_PASSED" in out, out[-4000:]
