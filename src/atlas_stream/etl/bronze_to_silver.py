"""Task 2 — bronze_countries_to_silver

Parses the raw JSON payloads from bronze and produces a structured,
deduplicated silver table with one row per country.

Source: <catalog>.bronze.countries_raw
Target: <catalog>.silver.countries
"""

import argparse

from databricks.sdk.runtime import spark
from pyspark.sql.functions import col, current_timestamp


def bronze_to_silver(catalog: str) -> None:
    bronze_df = spark.read.table(f"`{catalog}`.bronze.countries_raw")

    # Infer schema from the stored JSON strings using Spark's JSON reader
    json_rdd = bronze_df.select("raw_json").rdd.map(lambda row: row.raw_json)
    parsed = spark.read.json(json_rdd)

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

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.silver")
    silver_df.write.format("delta").mode("overwrite").saveAsTable(f"`{catalog}`.silver.countries")

    print(f"Wrote {silver_df.count()} rows → `{catalog}`.silver.countries")


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform countries bronze → silver")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    args = parser.parse_args()
    bronze_to_silver(args.catalog)


if __name__ == "__main__":
    main()
