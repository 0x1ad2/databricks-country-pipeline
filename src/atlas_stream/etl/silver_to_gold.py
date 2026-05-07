"""Task 3 — silver_countries_to_gold

Builds six gold tables from the silver countries data, each optimised for
a different analytical use-case.

Source:  <catalog>.silver.countries
Targets:
  gold.countries              — enriched country-level table (density, size category)
  gold.countries_by_region    — aggregated by region + subregion
  gold.countries_by_continent — aggregated by continent (region only)
  gold.population_tiers       — countries bucketed by population size
  gold.landlocked_vs_coastal  — landlocked vs coastal breakdown by region
  gold.un_membership_summary  — UN membership stats by region
"""

import argparse
import logging

from databricks.sdk.runtime import spark
from pyspark.sql.functions import col, count, current_timestamp, when
from pyspark.sql.functions import max as spark_max
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

_TABLE_COMMENTS = {
    "countries": (
        "Enriched country-level table with population_density (people/km²) and size_category bucket. "
        "Includes source_ingested_at for lineage. Source: REST Countries API v3.1. Refreshed daily."
    ),
    "countries_by_region": (
        "Country counts and population aggregated by region + subregion. "
        "Source: silver.countries. Refreshed daily."
    ),
    "countries_by_continent": (
        "Country counts and population aggregated by continent (region). "
        "Source: silver.countries. Refreshed daily."
    ),
    "population_tiers": (
        "Countries bucketed by population size: Large(≥100M), Medium(10M–100M), Small(1M–10M), Micro(<1M). "
        "Source: silver.countries. Refreshed daily."
    ),
    "landlocked_vs_coastal": (
        "Landlocked vs coastal country counts and population totals per region. "
        "Source: silver.countries. Refreshed daily."
    ),
    "un_membership_summary": (
        "UN member vs non-member country counts and population totals per region. "
        "Source: silver.countries. Refreshed daily."
    ),
}


def _write(df, catalog: str, table: str) -> None:
    """Overwrite a gold Delta table, apply COMMENT, OPTIMIZE, and report row count."""
    full_name = f"`{catalog}`.gold.{table}"
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(full_name)
    if table in _TABLE_COMMENTS:
        spark.sql(f"COMMENT ON TABLE {full_name} IS '{_TABLE_COMMENTS[table]}'")
    spark.sql(f"OPTIMIZE {full_name}")
    row_count = spark.read.table(full_name).count()
    logger.info("Wrote %d rows → %s", row_count, full_name)


def silver_to_gold(catalog: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.gold")

    silver_df = spark.read.table(f"`{catalog}`.silver.countries")

    # ------------------------------------------------------------------
    # gold.countries — enriched country-level table
    # Adds population_density (people / km²) and a size_category bucket.
    # area=null is explicitly handled to avoid misleading "Small" classification.
    # ------------------------------------------------------------------
    countries_df = silver_df.select(
        col("name_common"),
        col("name_official"),
        col("cca2"),
        col("cca3"),
        col("region"),
        col("subregion"),
        col("population"),
        col("area"),
        spark_round(
            when(col("area") > 0, col("population") / col("area")),
            2,
        ).alias("population_density"),
        when(col("area").isNull(), "Unknown")
        .when(col("area") > 1_000_000, "Very Large (>1M km2)")
        .when(col("area") > 100_000, "Large (100k-1M km2)")
        .when(col("area") > 10_000, "Medium (10k-100k km2)")
        .otherwise("Small (<10k km2)")
        .alias("size_category"),
        col("landlocked"),
        col("un_member"),
        col("source_ingested_at"),
        current_timestamp().alias("updated_at"),
    )
    _write(countries_df, catalog, "countries")

    # ------------------------------------------------------------------
    # gold.countries_by_region — aggregated by region + subregion
    # ------------------------------------------------------------------
    by_region_df = (
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
    _write(by_region_df, catalog, "countries_by_region")

    # ------------------------------------------------------------------
    # gold.countries_by_continent — continent (region) level rollup
    # ------------------------------------------------------------------
    by_continent_df = (
        silver_df.groupBy(col("region").alias("continent"))
        .agg(
            count("*").alias("country_count"),
            spark_sum("population").alias("total_population"),
            spark_sum("area").alias("total_area_km2"),
            spark_sum(col("un_member").cast("int")).alias("un_member_count"),
            spark_max("source_ingested_at").alias("source_ingested_at"),
        )
        .withColumn("updated_at", current_timestamp())
    )
    _write(by_continent_df, catalog, "countries_by_continent")

    # ------------------------------------------------------------------
    # gold.population_tiers — countries bucketed by population size
    # ------------------------------------------------------------------
    tiers_df = (
        silver_df.withColumn(
            "population_tier",
            when(col("population") >= 100_000_000, "Large (>=100M)")
            .when(col("population") >= 10_000_000, "Medium (10M-100M)")
            .when(col("population") >= 1_000_000, "Small (1M-10M)")
            .otherwise("Micro (<1M)"),
        )
        .groupBy("population_tier")
        .agg(
            count("*").alias("country_count"),
            spark_sum("population").alias("total_population"),
            spark_max("source_ingested_at").alias("source_ingested_at"),
        )
        .withColumn("updated_at", current_timestamp())
    )
    _write(tiers_df, catalog, "population_tiers")

    # ------------------------------------------------------------------
    # gold.landlocked_vs_coastal — landlocked vs coastal by region
    # ------------------------------------------------------------------
    landlocked_df = (
        silver_df.groupBy("region", "landlocked")
        .agg(
            count("*").alias("country_count"),
            spark_sum("population").alias("total_population"),
            spark_sum("area").alias("total_area_km2"),
            spark_max("source_ingested_at").alias("source_ingested_at"),
        )
        .withColumn("updated_at", current_timestamp())
    )
    _write(landlocked_df, catalog, "landlocked_vs_coastal")

    # ------------------------------------------------------------------
    # gold.un_membership_summary — UN membership stats by region
    # ------------------------------------------------------------------
    un_df = (
        silver_df.groupBy("region", "un_member")
        .agg(
            count("*").alias("country_count"),
            spark_sum("population").alias("total_population"),
            spark_max("source_ingested_at").alias("source_ingested_at"),
        )
        .withColumn("updated_at", current_timestamp())
    )
    _write(un_df, catalog, "un_membership_summary")


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform countries silver → gold")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    args = parser.parse_args()
    silver_to_gold(args.catalog)


if __name__ == "__main__":
    main()
