import pytest
from pyspark.sql import functions as F

from tri_transforms import (
    GRAMS_TO_POUNDS,
    flatten_json_records,
    normalize_column_name,
    normalize_columns,
    pick_columns,
    release_quality_metrics,
    standardize_facilities,
    standardize_releases,
    RELEASE_COLUMNS,
)


def _bronze_releases(spark, fixtures_dir):
    raw = (
        spark.read.option("header", True)
        .option("inferSchema", False)
        .option("escape", '"')
        .csv(str(fixtures_dir / "sample_releases.csv"))
    )
    raw = raw.select(
        "*",
        F.col("_metadata.file_path").alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )
    return normalize_columns(raw)


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("1. YEAR", "year"),
        ("48. 5.1 – FUGITIVE AIR", "5_1_fugitive_air"),
        ("5.1 - FUGITIVE AIR", "5_1_fugitive_air"),
        ("37. CAS #", "cas"),
        ("CAS#", "cas"),
        ("62. ON-SITE RELEASE TOTAL", "on_site_release_total"),
        ("TRI CHEMICAL/COMPOUND ID", "tri_chemical_compound_id"),
        ("FRS ID", "frs_id"),
        ("  ", "unnamed"),
    ],
)
def test_normalize_column_name(raw, expected):
    assert normalize_column_name(raw) == expected


def test_normalize_columns_dedupes_and_keeps_metadata(spark):
    df = spark.createDataFrame([("a", "b", "c")], ["CITY", "City ", "_source_file"])
    out = normalize_columns(df)
    assert out.columns == ["city", "city_1", "_source_file"]


def test_pick_columns_fails_loudly_on_missing_required(spark):
    df = spark.createDataFrame([("x",)], ["something_else"])
    with pytest.raises(ValueError) as exc:
        pick_columns(df, RELEASE_COLUMNS)
    assert "reporting_year" in str(exc.value)
    assert "something_else" in str(exc.value)


def test_standardize_releases(spark, fixtures_dir):
    bronze = _bronze_releases(spark, fixtures_dir)
    silver = standardize_releases(bronze)
    rows = {r["record_key"]: r for r in silver.collect()}

    # 6 input rows: 1 exact duplicate form removed, 1 row with no TRIFD dropped
    assert len(rows) == 4

    ethylene = rows["1324123456789"]
    assert ethylene["reporting_year"] == 2024
    assert ethylene["state"] == "MI"            # upper-cased
    assert ethylene["zip"] == "48640"           # ZIP+4 trimmed
    assert ethylene["fugitive_air_lb"] == pytest.approx(1200.5)  # "1,200.5" parsed
    assert ethylene["air_releases_lb"] == pytest.approx(4200.5)
    assert ethylene["carcinogen"] is False

    dioxin = rows["1324123456790"]
    assert dioxin["reported_in_grams"] is True
    assert dioxin["total_releases_lb"] == pytest.approx(2 * GRAMS_TO_POUNDS)
    assert dioxin["pbt"] is True

    pfoa = rows["1324999999999"]
    assert pfoa["pfas"] is True
    assert pfoa["latitude"] is None
    assert pfoa["parent_company"] is None       # "" -> NULL

    ghost = rows["1324000000001"]
    assert ghost["total_releases_lb"] is None   # "n/a" -> NULL, not a crash under ANSI


def test_quality_metrics(spark, fixtures_dir):
    bronze = _bronze_releases(spark, fixtures_dir)
    silver = standardize_releases(bronze)
    m = release_quality_metrics(bronze.count(), silver)
    assert m["bronze_rows"] == 6
    assert m["silver_rows"] == 4
    assert m["rows_dropped_or_deduplicated"] == 2
    assert m["rows_converted_from_grams"] == 1
    assert m["rows_missing_coordinates"] == 1
    assert m["rows_missing_parent"] == 1
    assert m["rows_unparseable_total"] == 1


def test_standardize_facilities(spark, fixtures_dir):
    raw = spark.read.option("multiLine", True).json(str(fixtures_dir / "sample_facilities.json"))
    fac = standardize_facilities(normalize_columns(flatten_json_records(raw)))
    rows = {r["trifd"]: r for r in fac.collect()}
    assert len(rows) == 3                       # duplicate registry row collapsed
    assert rows["77541FRPRT1234"]["registry_state"] == "TX"
    assert rows["77541FRPRT1234"]["is_closed"] is True
    assert rows["48640THDWCBLDG4"]["registry_latitude"] == pytest.approx(43.5936)
    assert rows["99999UNUSED0001"]["registry_parent_company"] is None


def test_flatten_wrapped_payload(spark, fixtures_dir):
    raw = spark.read.option("multiLine", True).json(str(fixtures_dir / "sample_facilities_wrapped.json"))
    flat = flatten_json_records(raw)
    assert sorted(r["tri_facility_id"] for r in flat.collect()) == ["A1", "B2"]
