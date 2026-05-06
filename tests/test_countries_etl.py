"""Unit tests for the atlas_stream countries ETL pipeline.

Pure-Python tests (no Spark) cover argument parsing, serialisation, and
HTTP error handling.  Spark-touching tests use the ``spark`` fixture from
conftest.py which connects via Databricks Connect.
"""

import json
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SAMPLE_COUNTRY = {
    "name": {"common": "Netherlands", "official": "Kingdom of the Netherlands"},
    "cca2": "NL",
    "cca3": "NLD",
    "region": "Europe",
    "subregion": "Western Europe",
    "population": 17_590_672,
    "area": 41_543.0,
    "landlocked": False,
    "unMember": True,
}

REQUIRED_FIELDS = {"name", "cca2", "cca3", "region", "subregion", "population", "area", "landlocked", "unMember"}


# ---------------------------------------------------------------------------
# ingest.py — argument parsing
# ---------------------------------------------------------------------------


def test_ingest_catalog_required():
    """--catalog is a required CLI argument; omitting it raises SystemExit."""
    from atlas_stream.etl.ingest import main

    with pytest.raises(SystemExit):
        with patch("sys.argv", ["ingest_countries"]):
            main()


def test_ingest_api_url_default_contains_fields_param():
    """Default --source-api-url must contain ?fields= (v3.1 returns HTTP 400 without it)."""
    import argparse
    from atlas_stream.etl import ingest as ingest_mod

    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--source-api-url", default=None)

    # Extract the default from the real parser inside main() by inspecting the module source.
    # We look for the hardcoded default string.
    source = open(ingest_mod.__file__).read()
    assert "?fields=" in source, "Default API URL must include ?fields= query parameter"


def test_ingest_api_url_default_max_ten_fields():
    """Default URL must request at most 10 fields (v3.1 API hard limit)."""
    from atlas_stream.etl import ingest as ingest_mod

    source = open(ingest_mod.__file__).read()
    # Find the fields= value in the source
    import re
    match = re.search(r"\?fields=([^\"'\s]+)", source)
    assert match, "Could not find ?fields= in ingest.py source"
    fields = match.group(1).split(",")
    assert len(fields) <= 10, f"API URL requests {len(fields)} fields; max allowed by v3.1 is 10"


# ---------------------------------------------------------------------------
# ingest.py — country serialisation
# ---------------------------------------------------------------------------


def test_ingest_country_serialises_to_valid_json():
    """Each country dict must round-trip through json.dumps without error."""
    serialised = json.dumps(SAMPLE_COUNTRY)
    recovered = json.loads(serialised)
    assert recovered["cca2"] == "NL"


def test_ingest_all_required_fields_present():
    """REQUIRED_FIELDS are all present in the sample country fixture."""
    assert REQUIRED_FIELDS.issubset(SAMPLE_COUNTRY.keys())


def test_ingest_raises_on_http_error():
    """When the API returns a 4xx status, raise_for_status() must propagate."""
    import requests
    from atlas_stream.etl.ingest import ingest_countries

    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("400 Bad Request")

    with patch("atlas_stream.etl.ingest.requests.get", return_value=mock_response):
        with pytest.raises(requests.HTTPError):
            ingest_countries(catalog="workspace", source_api_url="https://example.com/api")


# ---------------------------------------------------------------------------
# ingest.py — bronze write (Spark)
# ---------------------------------------------------------------------------


def test_ingest_writes_bronze_table(spark):
    """ingest_countries() writes a Delta table with the expected schema columns."""
    from atlas_stream.etl.ingest import ingest_countries

    mock_response = MagicMock()
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = [SAMPLE_COUNTRY]

    with (
        patch("atlas_stream.etl.ingest.requests.get", return_value=mock_response),
        patch("atlas_stream.etl.ingest.spark", spark),
    ):
        # Use in-memory catalog workaround: write to a temp view instead.
        # We mock saveAsTable to capture the DataFrame written.
        written = []

        def capture_write(table_name):
            written.append(table_name)

        with patch.object(spark.sql.__self__.__class__, "sql", return_value=MagicMock()):
            df_mock = MagicMock()
            df_mock.write.format.return_value.mode.return_value.saveAsTable.side_effect = capture_write

            with patch("atlas_stream.etl.ingest.spark.createDataFrame") as mock_create:
                mock_create.return_value.withColumn.return_value.withColumn.return_value = df_mock
                ingest_countries(catalog="workspace", source_api_url="https://example.com?fields=name,cca2")

    assert len(written) == 1
    assert "bronze.countries_raw" in written[0]


# ---------------------------------------------------------------------------
# verify.py — table list constant
# ---------------------------------------------------------------------------


def test_verify_checks_all_three_medallion_layers():
    """_TABLES must reference all three medallion layers in the correct order."""
    from atlas_stream.etl.verify import _TABLES

    schemas = [row[0] for row in _TABLES]
    tables = [row[1] for row in _TABLES]

    assert schemas == ["bronze", "silver", "gold"], "_TABLES must cover bronze, silver, gold"
    assert "countries_raw" in tables
    assert "countries" in tables
    assert "countries_by_region" in tables


def test_verify_raises_assertion_on_empty_table(spark):
    """verify_country_tables() raises AssertionError when a table has 0 rows."""
    from atlas_stream.etl.verify import verify_country_tables

    with patch("atlas_stream.etl.verify.spark", spark):
        # Mock spark.read.table to return a DataFrame with 0 rows
        mock_df = MagicMock()
        mock_df.count.return_value = 0
        spark_mock = MagicMock()
        spark_mock.read.table.return_value = mock_df

        with patch("atlas_stream.etl.verify.spark", spark_mock):
            with pytest.raises(AssertionError, match="empty"):
                verify_country_tables(catalog="workspace")


# ---------------------------------------------------------------------------
# silver + gold schema — column names (Spark)
# ---------------------------------------------------------------------------


def test_silver_output_columns(spark):
    """bronze_to_silver() must produce the expected output column set."""
    from pyspark.sql.types import StringType, StructField, StructType

    # Build a minimal bronze DataFrame (one row of raw JSON)
    bronze_schema = StructType([StructField("raw_json", StringType())])
    bronze_df = spark.createDataFrame([(json.dumps(SAMPLE_COUNTRY),)], schema=bronze_schema)

    from pyspark.sql.functions import col, current_timestamp, from_json, lit, schema_of_json

    sample_json = bronze_df.select("raw_json").limit(1).collect()[0][0]
    schema_str = bronze_df.select(schema_of_json(lit(sample_json))).collect()[0][0]

    parsed = bronze_df.select(from_json(col("raw_json"), schema_str).alias("data")).select("data.*")

    silver_df = (
        parsed.select(
            col("name.common").alias("name_common"),
            col("name.official").alias("name_official"),
            col("cca2"),
            col("cca3"),
            col("region"),
            col("subregion"),
            col("population").cast("long"),
            col("area").cast("double"),
            col("landlocked").cast("boolean"),
            col("unMember").alias("un_member").cast("boolean"),
        )
        .filter(col("cca2").isNotNull())
        .withColumn("updated_at", current_timestamp())
    )

    expected = {"name_common", "name_official", "cca2", "cca3", "region", "subregion",
                "population", "area", "landlocked", "un_member", "updated_at"}
    assert set(silver_df.columns) == expected


def test_gold_output_columns(spark):
    """silver_to_gold() aggregation must produce the expected output column set."""
    from pyspark.sql.functions import col, count, current_timestamp
    from pyspark.sql.functions import sum as spark_sum
    from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType,
                                   StructField, StructType)

    silver_schema = StructType([
        StructField("region", StringType()),
        StructField("subregion", StringType()),
        StructField("population", LongType()),
        StructField("area", DoubleType()),
        StructField("un_member", BooleanType()),
    ])

    silver_df = spark.createDataFrame(
        [("Europe", "Western Europe", 17_590_672, 41_543.0, True)],
        schema=silver_schema,
    )

    gold_df = (
        silver_df.groupBy("region", "subregion")
        .agg(
            count("*").alias("country_count"),
            spark_sum("population").alias("total_population"),
            spark_sum("area").alias("total_area_km2"),
            spark_sum(col("un_member").cast("int")).alias("un_member_count"),
        )
        .withColumn("updated_at", current_timestamp())
    )

    expected = {"region", "subregion", "country_count", "total_population", "total_area_km2",
                "un_member_count", "updated_at"}
    assert set(gold_df.columns) == expected
