"""Task 3 — silver_countries_to_gold

Aggregates the silver countries table into a regional summary suitable
for analytics and reporting.

Source: <catalog>.silver.countries
Target: <catalog>.gold.countries_by_region
"""

import argparse

from databricks.sdk.runtime import spark
from pyspark.sql.functions import col, count, current_timestamp
from pyspark.sql.functions import sum as spark_sum


def silver_to_gold(catalog: str) -> None:
    silver_df = spark.read.table(f"`{catalog}`.silver.countries")

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

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.gold")
    gold_df.write.format("delta").mode("overwrite").saveAsTable(f"`{catalog}`.gold.countries_by_region")

    print(f"Wrote {gold_df.count()} rows → `{catalog}`.gold.countries_by_region")


def main() -> None:
    parser = argparse.ArgumentParser(description="Transform countries silver → gold")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    args = parser.parse_args()
    silver_to_gold(args.catalog)


if __name__ == "__main__":
    main()
