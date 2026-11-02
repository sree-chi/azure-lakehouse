"""Pure PySpark transforms for the TRI lakehouse.

No dbutils or Databricks-only APIs live here, so this module is unit-tested
locally (tests/test_transforms.py) and imported by the notebooks as a
workspace file.
"""

from __future__ import annotations

import re
from typing import Dict, Iterable, List, Sequence, Tuple

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StructType

GRAMS_TO_POUNDS = 0.00220462262185

# Newer TRI basic data files number their headers ("1. YEAR", "48. 5.1 - FUGITIVE AIR").
# Strip only that leading "<n>. " so "5.1 - FUGITIVE AIR" keeps its section number.
_HEADER_NUMBER_PREFIX = re.compile(r"^\s*\d+\.\s+")
_NON_ALNUM = re.compile(r"[^a-z0-9]+")

# target column -> (candidate source names after normalization, required?)
ColumnSpec = Dict[str, Tuple[Sequence[str], bool]]

RELEASE_COLUMNS: ColumnSpec = {
    "reporting_year": (["year", "reporting_year"], True),
    "trifd": (["trifd", "tri_facility_id"], True),
    "frs_id": (["frs_id"], False),
    "facility_name": (["facility_name"], True),
    "city": (["city", "city_name"], False),
    "county": (["county", "county_name"], False),
    "state": (["st", "state", "state_abbr"], True),
    "zip": (["zip", "zip_code"], False),
    "latitude": (["latitude"], False),
    "longitude": (["longitude"], False),
    "parent_company": (["parent_co_name", "parent_company_name"], False),
    "standardized_parent_company": (
        [
            "standardized_parent_company_name",
            "standardized_parent_co_name",
            "standard_parent_co_name",
            "standard_parent_company_name",
        ],
        False,
    ),
    "industry_sector_code": (["industry_sector_code"], False),
    "industry_sector": (["industry_sector"], False),
    "primary_naics": (["primary_naics"], False),
    "doc_ctrl_num": (["doc_ctrl_num"], False),
    "chemical": (["chemical", "chem_name"], True),
    "cas_number": (["cas", "cas_number", "cas_registry_number"], False),
    "carcinogen": (["carcinogen"], False),
    "pbt": (["pbt", "pbt_ind"], False),
    "pfas": (["pfas", "pfas_ind"], False),
    "unit_of_measure": (["unit_of_measure"], True),
    "fugitive_air": (["5_1_fugitive_air"], False),
    "stack_air": (["5_2_stack_air"], False),
    "water": (["5_3_water"], False),
    "on_site_release_total": (["on_site_release_total"], False),
    "off_site_release_total": (["off_site_release_total"], False),
    "total_releases": (["total_releases"], True),
}

FACILITY_COLUMNS: ColumnSpec = {
    "trifd": (["tri_facility_id", "trifd"], True),
    "registry_facility_name": (["facility_name", "fac_name"], False),
    "registry_state": (["state_abbr", "st", "state"], False),
    "registry_county": (["county_name", "county"], False),
    "registry_parent_company": (["parent_co_name", "parent_company_name"], False),
    "closed_indicator": (["fac_closed_ind", "closed_ind"], False),
    "registry_latitude": (["pref_latitude", "fac_latitude", "latitude"], False),
    "registry_longitude": (["pref_longitude", "fac_longitude", "longitude"], False),
}

RELEASE_QUANTITY_COLUMNS = [
    "fugitive_air",
    "stack_air",
    "water",
    "on_site_release_total",
    "off_site_release_total",
    "total_releases",
]

METADATA_COLUMNS = ("_source_file", "_ingested_at", "_ingest_date")


# --------------------------------------------------------------------------- #
# Column-name handling
# --------------------------------------------------------------------------- #
def normalize_column_name(name: str) -> str:
    """'48. 5.1 – FUGITIVE AIR' -> '5_1_fugitive_air'; 'CAS #' -> 'cas'."""
    cleaned = _HEADER_NUMBER_PREFIX.sub("", name).strip().lower()
    cleaned = _NON_ALNUM.sub("_", cleaned).strip("_")
    return cleaned or "unnamed"


def _quote(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def normalize_columns(df: DataFrame, keep: Iterable[str] = METADATA_COLUMNS) -> DataFrame:
    """Snake-case every column (Delta-safe), de-duplicating collisions.

    Columns listed in `keep` (ingestion metadata) are passed through unchanged.
    """
    keep = set(keep)
    seen: Dict[str, int] = {}
    exprs: List[Column] = []
    for col_name in df.columns:
        if col_name in keep:
            exprs.append(F.col(_quote(col_name)))
            continue
        new = normalize_column_name(col_name)
        if new in seen:
            seen[new] += 1
            new = f"{new}_{seen[new]}"
        else:
            seen[new] = 0
        exprs.append(F.col(_quote(col_name)).alias(new))
    return df.select(*exprs)


def flatten_json_records(df: DataFrame) -> DataFrame:
    """Unwrap API payloads shaped like {"Results": [{...}, ...]} into one row per record.

    A top-level JSON array is already one row per record, so it passes through.
    """
    fields = df.schema.fields
    if len(fields) == 1:
        dtype = fields[0].dataType
        name = _quote(fields[0].name)
        if isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
            return df.select(F.explode(F.col(name)).alias("_r")).select("_r.*")
        if isinstance(dtype, StructType):
            return df.select(f"{name}.*")
    return df


def pick_columns(df: DataFrame, spec: ColumnSpec) -> DataFrame:
    """Select spec targets from whichever candidate name exists.

    Missing optional columns become typed NULLs. Missing required columns fail
    loudly and list what *is* available, so schema drift is caught at silver.
    """
    available = set(df.columns)
    exprs: List[Column] = []
    missing: List[str] = []
    for target, (candidates, required) in spec.items():
        found = next((c for c in candidates if c in available), None)
        if found is not None:
            exprs.append(F.col(_quote(found)).cast("string").alias(target))
        elif required:
            missing.append(f"{target} (looked for {list(candidates)})")
        else:
            exprs.append(F.lit(None).cast("string").alias(target))
    if missing:
        raise ValueError(
            "Source schema is missing required columns: "
            + "; ".join(missing)
            + f". Available columns: {sorted(available)}"
        )
    exprs.extend(F.col(c) for c in METADATA_COLUMNS if c in available)
    return df.select(*exprs)


# --------------------------------------------------------------------------- #
# Value cleaning
# --------------------------------------------------------------------------- #
def blank_to_null(col: Column) -> Column:
    trimmed = F.trim(col)
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def to_double(col_name: str) -> Column:
    """'1,234.5' -> 1234.5; junk -> NULL (try_cast is safe under ANSI mode)."""
    return F.expr(f"try_cast(regexp_replace(trim({_quote(col_name)}), ',', '') as double)")


def to_int(col_name: str) -> Column:
    return F.expr(f"try_cast(trim({_quote(col_name)}) as int)")


def to_bool_flag(col_name: str) -> Column:
    v = F.upper(F.trim(F.col(_quote(col_name))))
    return (
        F.when(v.isin("YES", "Y", "TRUE", "T", "1"), F.lit(True))
        .when(v.isin("NO", "N", "FALSE", "F", "0"), F.lit(False))
        .otherwise(F.lit(None).cast("boolean"))
    )


def _latest_per_key(df: DataFrame, key: str) -> DataFrame:
    order = F.col("_ingested_at").desc_nulls_last() if "_ingested_at" in df.columns else F.lit(1)
    w = Window.partitionBy(key).orderBy(order)
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


# --------------------------------------------------------------------------- #
# Silver builders
# --------------------------------------------------------------------------- #
def standardize_releases(bronze: DataFrame) -> DataFrame:
    """Bronze (all strings, snake_case) -> typed, unit-normalized, de-duplicated releases.

    * Dioxins are reported in grams; everything is converted to pounds (*_lb).
    * One row per TRI reporting form (doc_ctrl_num); re-ingesting the same year
      keeps only the latest copy, so the job is safe to re-run.
    """
    df = pick_columns(bronze, RELEASE_COLUMNS)

    string_cols = [
        "trifd", "frs_id", "facility_name", "city", "county", "parent_company",
        "standardized_parent_company", "industry_sector_code", "industry_sector",
        "primary_naics", "doc_ctrl_num", "chemical", "cas_number", "unit_of_measure",
    ]
    for c in string_cols:
        df = df.withColumn(c, blank_to_null(F.col(c)))

    df = (
        df.withColumn("reporting_year", to_int("reporting_year"))
        .withColumn("state", F.upper(blank_to_null(F.col("state"))))
        .withColumn(
            "zip",
            blank_to_null(F.substring(F.regexp_replace(F.col("zip"), "[^0-9]", ""), 1, 5)),
        )
        .withColumn("latitude", to_double("latitude"))
        .withColumn("longitude", to_double("longitude"))
        .withColumn("carcinogen", to_bool_flag("carcinogen"))
        .withColumn("pbt", to_bool_flag("pbt"))
        .withColumn("pfas", to_bool_flag("pfas"))
        .withColumn(
            "reported_in_grams",
            F.coalesce(F.upper(F.col("unit_of_measure")).startswith("GRAM"), F.lit(False)),
        )
    )

    for c in RELEASE_QUANTITY_COLUMNS:
        qty = to_double(c)
        df = df.withColumn(
            f"{c}_lb",
            F.when(F.col("reported_in_grams"), qty * F.lit(GRAMS_TO_POUNDS)).otherwise(qty),
        ).drop(c)

    df = df.withColumn(
        "air_releases_lb",
        F.coalesce(F.col("fugitive_air_lb"), F.lit(0.0)) + F.coalesce(F.col("stack_air_lb"), F.lit(0.0)),
    ).withColumn(
        "record_key",
        F.coalesce(
            F.col("doc_ctrl_num"),
            F.concat_ws("|", F.col("trifd"), F.col("chemical"), F.col("reporting_year").cast("string")),
        ),
    )

    df = df.filter(F.col("trifd").isNotNull() & F.col("reporting_year").isNotNull())
    return _latest_per_key(df, "record_key")


def standardize_facilities(bronze: DataFrame) -> DataFrame:
    """Facility registry snapshot -> one row per TRI facility ID."""
    df = pick_columns(bronze, FACILITY_COLUMNS)
    for c in ["trifd", "registry_facility_name", "registry_county", "registry_parent_company"]:
        df = df.withColumn(c, blank_to_null(F.col(c)))
    df = (
        df.withColumn("registry_state", F.upper(blank_to_null(F.col("registry_state"))))
        .withColumn("is_closed", to_bool_flag("closed_indicator"))
        .drop("closed_indicator")
        .withColumn("registry_latitude", to_double("registry_latitude"))
        .withColumn("registry_longitude", to_double("registry_longitude"))
        .filter(F.col("trifd").isNotNull())
    )
    return _latest_per_key(df, "trifd")


def release_quality_metrics(bronze_rows: int, silver: DataFrame) -> Dict[str, int]:
    """Row-level checks recorded on every run (written to silver.dq_metrics)."""
    row = silver.agg(
        F.count(F.lit(1)).alias("silver_rows"),
        F.sum(F.col("reported_in_grams").cast("int")).alias("rows_converted_from_grams"),
        F.sum((F.col("latitude").isNull() | F.col("longitude").isNull()).cast("int")).alias(
            "rows_missing_coordinates"
        ),
        F.sum(
            (F.col("parent_company").isNull() & F.col("standardized_parent_company").isNull()).cast("int")
        ).alias("rows_missing_parent"),
        F.sum((F.col("total_releases_lb") < 0).cast("int")).alias("rows_negative_releases"),
        F.sum(F.col("total_releases_lb").isNull().cast("int")).alias("rows_unparseable_total"),
    ).collect()[0]
    metrics = {k: int(v or 0) for k, v in row.asDict().items()}
    metrics["bronze_rows"] = int(bronze_rows)
    metrics["rows_dropped_or_deduplicated"] = int(bronze_rows) - metrics["silver_rows"]
    return metrics
