"""Unit tests for the atlas_stream countries ETL pipeline.

Pure-Python tests (no Spark) cover argument parsing, serialisation,
HTTP session config, response validation, and schema contracts.
Spark tests use the ``spark`` fixture from conftest.py and are skipped
when Databricks Connect is unavailable (CI without credentials).

Mark Spark tests with @pytest.mark.spark to allow easy selective execution:
    uv run pytest -m "not spark"
"""

import json
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Shared fixtures / constants
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
    import atlas_stream.etl.ingest as ingest_mod

    source = open(ingest_mod.__file__).read()
    assert "?fields=" in source, "Default API URL must include ?fields= query parameter"


# ---------------------------------------------------------------------------
# ingest.py — HTTP session configuration
# ---------------------------------------------------------------------------


def test_ingest_session_has_retry_adapter():
    """_build_session must mount an HTTPAdapter with retry configuration."""
    from requests.adapters import HTTPAdapter
    from atlas_stream.etl.ingest import _build_session

    session = _build_session()
    adapter = session.get_adapter("https://example.com")
    assert isinstance(adapter, HTTPAdapter)
    assert adapter.max_retries.total == 3


def test_ingest_request_timeout_is_tuple():
    """_REQUEST_TIMEOUT must be a (connect, read) tuple to prevent indefinite hangs."""
    from atlas_stream.etl.ingest import _REQUEST_TIMEOUT

    assert isinstance(_REQUEST_TIMEOUT, tuple), "_REQUEST_TIMEOUT must be a tuple, not a scalar"
    assert len(_REQUEST_TIMEOUT) == 2
    connect_t, read_t = _REQUEST_TIMEOUT
    assert 0 < connect_t <= 10, "connect timeout should be 1–10 seconds"
    assert 0 < read_t <= 60, "read timeout should be 1–60 seconds"


# ---------------------------------------------------------------------------
# ingest.py — response validation
# ---------------------------------------------------------------------------


def test_validate_response_rejects_non_list():
    from atlas_stream.etl.ingest import _validate_response

    with pytest.raises(ValueError, match="Expected JSON array"):
        _validate_response({"error": "message"}, "https://api.example.com")


def test_validate_response_rejects_empty_list():
    from atlas_stream.etl.ingest import _validate_response

    with pytest.raises(ValueError, match="empty array"):
        _validate_response([], "https://api.example.com")


def test_validate_response_rejects_missing_fields():
    from atlas_stream.etl.ingest import _validate_response

    with pytest.raises(ValueError, match="missing expected fields"):
        _validate_response([{"name": "test"}], "https://api.example.com")


def test_validate_response_accepts_valid_payload():
    from atlas_stream.etl.ingest import _validate_response

    # Should not raise
    _validate_response([SAMPLE_COUNTRY], "https://api.example.com")


# ---------------------------------------------------------------------------
# ingest.py — HTTP error handling
# ---------------------------------------------------------------------------


def test_http_error_raises():
    """A non-2xx response from the API propagates as an HTTPError."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("HTTP 503")

    with patch("atlas_stream.etl.ingest._build_session") as mock_session_factory:
        mock_session = MagicMock()
        mock_session.get.return_value = mock_response
        mock_session_factory.return_value = mock_session

        from atlas_stream.etl.ingest import ingest_countries

        with pytest.raises(Exception, match="HTTP 503"):
            ingest_countries("test_catalog", "https://api.example.com/countries")


# ---------------------------------------------------------------------------
# ingest.py — JSON serialisation
# ---------------------------------------------------------------------------


def test_country_serialisation_round_trips():
    """JSON serialise → deserialise must produce the original dict."""
    serialised = json.dumps(SAMPLE_COUNTRY)
    deserialised = json.loads(serialised)
    assert deserialised == SAMPLE_COUNTRY


def test_all_required_fields_present():
    """Sample country dict must contain all fields the pipeline expects."""
    assert REQUIRED_FIELDS.issubset(SAMPLE_COUNTRY.keys())


# ---------------------------------------------------------------------------
# schema.py — contract tests
# ---------------------------------------------------------------------------


def test_country_row_schema_contains_required_fields():
    """COUNTRY_ROW_SCHEMA must cover all fields needed by bronze_to_silver.py."""
    from atlas_stream.etl.schema import COUNTRY_ROW_SCHEMA

    schema_top_fields = {f.name for f in COUNTRY_ROW_SCHEMA.fields}
    # name is a nested struct; all other required fields should be top-level
    required_top = {"name", "cca2", "cca3", "region", "subregion", "population", "area", "landlocked", "unMember"}
    assert required_top.issubset(schema_top_fields), (
        f"COUNTRY_ROW_SCHEMA missing fields: {required_top - schema_top_fields}"
    )


def test_country_row_schema_name_nested():
    """COUNTRY_ROW_SCHEMA.name must be a nested StructType with common and official."""
    from pyspark.sql.types import StructType
    from atlas_stream.etl.schema import COUNTRY_ROW_SCHEMA

    name_field = next(f for f in COUNTRY_ROW_SCHEMA.fields if f.name == "name")
    assert isinstance(name_field.dataType, StructType)
    nested_names = {f.name for f in name_field.dataType.fields}
    assert {"common", "official"}.issubset(nested_names)


# ---------------------------------------------------------------------------
# verify.py — table list and quality constants
# ---------------------------------------------------------------------------


def test_verify_table_list():
    """_TABLES must cover all eight expected medallion tables."""
    from atlas_stream.etl.verify import _TABLES

    assert ("bronze", "countries_raw") in _TABLES
    assert ("silver", "countries") in _TABLES
    assert ("gold", "countries") in _TABLES
    assert ("gold", "countries_by_region") in _TABLES
    assert ("gold", "countries_by_continent") in _TABLES
    assert ("gold", "population_tiers") in _TABLES
    assert ("gold", "landlocked_vs_coastal") in _TABLES
    assert ("gold", "un_membership_summary") in _TABLES
    assert len(_TABLES) == 8


def test_verify_silver_count_bounds():
    """Silver count range constants must reflect the ~250-country REST Countries API.

    Note: ~5 uninhabited territories (Antarctica etc.) have population=0,
    which is valid. The check only rejects *negative* population.
    """
    from atlas_stream.etl.verify import _SILVER_COUNT_MIN, _SILVER_COUNT_MAX

    assert _SILVER_COUNT_MIN == 220
    assert _SILVER_COUNT_MAX == 260
    assert _SILVER_COUNT_MIN < _SILVER_COUNT_MAX


# ---------------------------------------------------------------------------
# Spark tests — require Databricks Connect (skipped in CI without credentials)
# ---------------------------------------------------------------------------


@pytest.mark.spark
def test_silver_output_columns(spark):
    """bronze_to_silver logic must produce the expected output column set."""
    from pyspark.sql.functions import col, current_timestamp, from_json
    from pyspark.sql.types import StringType, StructField, StructType, TimestampType

    from atlas_stream.etl.schema import COUNTRY_ROW_SCHEMA

    # Build a minimal bronze DataFrame with raw_json + ingested_at
    bronze_schema = StructType([
        StructField("raw_json", StringType()),
        StructField("ingested_at", TimestampType()),
    ])
    from pyspark.sql.functions import current_timestamp as ts_now
    bronze_df = spark.createDataFrame(
        [(json.dumps(SAMPLE_COUNTRY), None)], schema=bronze_schema
    ).withColumn("ingested_at", ts_now())

    silver_df = (
        bronze_df
        .withColumn("data", from_json(col("raw_json"), COUNTRY_ROW_SCHEMA))
        .select(
            col("data.name.common").alias("name_common"),
            col("data.name.official").alias("name_official"),
            col("data.cca2").alias("cca2"),
            col("data.cca3").alias("cca3"),
            col("data.region").alias("region"),
            col("data.subregion").alias("subregion"),
            col("data.population").cast("long").alias("population"),
            col("data.area").cast("double").alias("area"),
            col("data.landlocked").cast("boolean").alias("landlocked"),
            col("data.unMember").cast("boolean").alias("un_member"),
            col("ingested_at").alias("source_ingested_at"),
        )
        .filter(col("cca2").isNotNull())
        .withColumn("updated_at", current_timestamp())
    )

    expected = {
        "name_common", "name_official", "cca2", "cca3", "region", "subregion",
        "population", "area", "landlocked", "un_member", "source_ingested_at", "updated_at",
    }
    assert set(silver_df.columns) == expected


@pytest.mark.spark
def test_gold_countries_output_columns(spark):
    """gold.countries must include enriched fields: population_density, size_category, source_ingested_at."""
    from pyspark.sql.functions import col, current_timestamp, when
    from pyspark.sql.functions import round as spark_round
    from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType,
                                   StructField, StructType, TimestampType)

    silver_schema = StructType([
        StructField("name_common", StringType()),
        StructField("name_official", StringType()),
        StructField("cca2", StringType()),
        StructField("cca3", StringType()),
        StructField("region", StringType()),
        StructField("subregion", StringType()),
        StructField("population", LongType()),
        StructField("area", DoubleType()),
        StructField("landlocked", BooleanType()),
        StructField("un_member", BooleanType()),
        StructField("source_ingested_at", TimestampType()),
        StructField("updated_at", TimestampType()),
    ])

    silver_df = spark.createDataFrame(
        [("Netherlands", "Kingdom of the Netherlands", "NL", "NLD",
          "Europe", "Western Europe", 17_590_672, 41_543.0, False, True, None, None)],
        schema=silver_schema,
    )

    gold_df = silver_df.select(
        col("name_common"), col("name_official"), col("cca2"), col("cca3"),
        col("region"), col("subregion"), col("population"), col("area"),
        spark_round(when(col("area") > 0, col("population") / col("area")), 2).alias("population_density"),
        when(col("area").isNull(), "Unknown")
        .when(col("area") > 1_000_000, "Very Large (>1M km2)")
        .when(col("area") > 100_000, "Large (100k-1M km2)")
        .when(col("area") > 10_000, "Medium (10k-100k km2)")
        .otherwise("Small (<10k km2)").alias("size_category"),
        col("landlocked"), col("un_member"),
        col("source_ingested_at"),
        current_timestamp().alias("updated_at"),
    )

    expected = {
        "name_common", "name_official", "cca2", "cca3", "region", "subregion",
        "population", "area", "population_density", "size_category",
        "landlocked", "un_member", "source_ingested_at", "updated_at",
    }
    assert set(gold_df.columns) == expected

    row = gold_df.collect()[0]
    assert row["size_category"] == "Medium (10k-100k km2)"
    assert row["population_density"] > 0


@pytest.mark.spark
def test_gold_countries_size_category_null_area(spark):
    """size_category must be 'Unknown' when area is null (not misleadingly 'Small')."""
    from pyspark.sql.functions import col, when
    from pyspark.sql.types import DoubleType, LongType, StringType, StructField, StructType

    schema = StructType([
        StructField("name_common", StringType()),
        StructField("area", DoubleType()),
        StructField("population", LongType()),
    ])
    df = spark.createDataFrame([("Vatican", None, 800)], schema=schema)

    result = df.select(
        when(col("area").isNull(), "Unknown")
        .when(col("area") > 1_000_000, "Very Large (>1M km2)")
        .when(col("area") > 100_000, "Large (100k-1M km2)")
        .when(col("area") > 10_000, "Medium (10k-100k km2)")
        .otherwise("Small (<10k km2)")
        .alias("size_category")
    ).collect()[0]["size_category"]

    assert result == "Unknown"


@pytest.mark.spark
def test_gold_countries_by_region_output_columns(spark):
    """gold.countries_by_region must produce the expected aggregation columns."""
    from pyspark.sql.functions import col, count, current_timestamp
    from pyspark.sql.functions import max as spark_max
    from pyspark.sql.functions import sum as spark_sum
    from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType,
                                   StructField, StructType, TimestampType)

    silver_schema = StructType([
        StructField("region", StringType()),
        StructField("subregion", StringType()),
        StructField("population", LongType()),
        StructField("area", DoubleType()),
        StructField("un_member", BooleanType()),
        StructField("source_ingested_at", TimestampType()),
    ])

    silver_df = spark.createDataFrame(
        [("Europe", "Western Europe", 17_590_672, 41_543.0, True, None)],
        schema=silver_schema,
    )

    gold_df = (
        silver_df.groupBy("region", "subregion")
        .agg(
            count("*").alias("country_count"),
            spark_sum("population").alias("total_population"),
            spark_sum("area").alias("total_area_km2"),
            spark_sum(col("un_member").cast("int")).alias("un_member_count"),
            spark_max("source_ingested_at").alias("source_ingested_at"),
        )
        .withColumn("updated_at", current_timestamp())
    )

    expected = {
        "region", "subregion", "country_count", "total_population",
        "total_area_km2", "un_member_count", "source_ingested_at", "updated_at",
    }
    assert set(gold_df.columns) == expected
