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

from databricks.sdk.runtime import spark
from pyspark.sql.functions import col, count, current_timestamp, when
from pyspark.sql.functions import round as spark_round
from pyspark.sql.functions import sum as spark_sum


def _write(df, catalog: str, table: str) -> None:
    """Overwrite a gold Delta table and report the row count."""
    full_name = f"`{catalog}`.gold.{table}"
    df.write.format("delta").mode("overwrite").saveAsTable(full_name)
    # Read count from the written table — avoids re-executing the transformation plan.
    row_count = spark.read.table(full_name).count()
    print(f"Wrote {row_count} rows \u2192 {full_name}")


def silver_to_gold(catalog: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.gold")

    silver_df = spark.read.table(f"`{catalog}`.silver.countries")

    # ------------------------------------------------------------------
    # gold.countries — enriched country-level table
    # Adds population_density (people / km²) and a size_category bucket.
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
        when(col("area") > 1_000_000, "Very Large (>1M km2)")
        .when(col("area") > 100_000, "Large (100k-1M km2)")
        .when(col("area") > 10_000, "Medium (10k-100k km2)")
        .otherwise("Small (<10k km2)")
        .alias("size_category"),
        col("landlocked"),
        col("un_member"),
        col("updated_at"),
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
        )
        .withColumn("updated_at", current_timestamp())
    )
    _write(by_region_df, catalog, "countries_by_region")

    # ------------------------------------------------------------------
    # gold.countries_by_continent — continent (region) level rollup
    # Collapses subregions into a single continent summary row.
    # ------------------------------------------------------------------
    by_continent_df = (
        silver_df.groupBy(col("region").alias("continent"))
        .agg(
            count("*").alias("country_count"),
            spark_sum("population").alias("total_population"),
            spark_sum("area").alias("total_area_km2"),
            spark_sum(col("un_member").cast("int")).alias("un_member_count"),
        )
        .withColumn("updated_at", current_timestamp())
    )
    _write(by_continent_df, catalog, "countries_by_continent")

    # ------------------------------------------------------------------
    # gold.population_tiers — countries bucketed by population size
    # Useful for understanding how many micro/small/medium/large nations exist.
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
        )
        .withColumn("updated_at", current_timestamp())
    )
    _write(tiers_df, catalog, "population_tiers")

    # ------------------------------------------------------------------
    # gold.landlocked_vs_coastal — landlocked vs coastal by region
    # Answers: which regions have the most landlocked nations?
    # ------------------------------------------------------------------
    landlocked_df = (
        silver_df.groupBy("region", "landlocked")
        .agg(
            count("*").alias("country_count"),
            spark_sum("population").alias("total_population"),
            spark_sum("area").alias("total_area_km2"),
        )
        .withColumn("updated_at", current_timestamp())
    )
    _write(landlocked_df, catalog, "landlocked_vs_coastal")

    # ------------------------------------------------------------------
    # gold.un_membership_summary — UN membership stats by region
    # Answers: how many countries per region are / are not UN members?
    # ------------------------------------------------------------------
    un_df = (
        silver_df.groupBy("region", "un_member")
        .agg(
            count("*").alias("country_count"),
            spark_sum("population").alias("total_population"),
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
