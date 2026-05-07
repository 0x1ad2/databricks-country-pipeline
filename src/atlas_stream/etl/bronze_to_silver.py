"""Task 2 — bronze_countries_to_silver

Parses the raw JSON payloads from the latest bronze batch and produces a
structured silver table with one row per country.

Changes from raw bronze:
- Explicit StructType schema (no single-row schema inference)
- Filters to the most recent ingested_at batch
- Logs and enforces a maximum drop rate on the cca2 filter
- Carries source_ingested_at for data-lineage tracking

Source: <catalog>.bronze.countries_raw
Target: <catalog>.silver.countries
"""

import argparse
import logging

from databricks.sdk.runtime import spark
from pyspark.sql.functions import col, current_timestamp, from_json, lit
from pyspark.sql.functions import max as spark_max

from atlas_stream.etl.schema import COUNTRY_ROW_SCHEMA

logging.basicConfig(
level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

# Fail the task if more than this fraction of rows are dropped on the cca2 filter
_MAX_DROP_FRACTION = 0.02


def bronze_to_silver(catalog: str) -> None:
    bronze_df = spark.read.table(f"`{catalog}`.bronze.countries_raw")

    # Filter to the most recent batch only
    latest_ts = bronze_df.agg(spark_max("ingested_at")).collect()[0][0]
    bronze_df = bronze_df.filter(col("ingested_at") == lit(latest_ts))
    logger.info("Processing bronze batch ingested_at=%s", latest_ts)

    # Parse JSON using an explicit schema — avoids brittle single-row inference
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
    )

    # Count drops on the cca2 filter and fail if the drop rate is too high
    total = silver_df.count()
    silver_df = silver_df.filter(col("cca2").isNotNull()).withColumn("updated_at", current_timestamp())
    kept = silver_df.count()
    dropped = total - kept
    if total > 0:
        drop_fraction = dropped / total
        logger.info("cca2 filter: kept %d / %d rows (dropped %d, %.1f%%)", kept, total, dropped, drop_fraction * 100)
        if drop_fraction > _MAX_DROP_FRACTION:
            raise ValueError(
                f"cca2 filter dropped {dropped}/{total} rows ({drop_fraction:.1%}) "
                f"— exceeds threshold of {_MAX_DROP_FRACTION:.0%}. Investigate API changes."
            )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.silver")
    silver_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
        f"`{catalog}`.silver.countries"
    )
    spark.sql(
        f"COMMENT ON TABLE `{catalog}`.silver.countries IS "
        "'Structured country data parsed from bronze.countries_raw. "
        "One row per country keyed by cca2. "
        "Includes source_ingested_at for lineage tracking. "
        "Source: REST Countries API v3.1. Refreshed daily.'"
    )
    spark.sql(f"OPTIMIZE `{catalog}`.silver.countries")

    row_count = spark.read.table(f"`{catalog}`.silver.countries").count()
    logger.info("Wrote %d rows → `%s`.silver.countries", row_count, catalog)


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform countries bronze → silver")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    args = parser.parse_args()
    bronze_to_silver(args.catalog)


if __name__ == "__main__":
    main()
