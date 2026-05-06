"""Task 1 — ingest_countries_to_bronze

Fetches all countries from the REST Countries API and writes each country's
raw JSON payload as a single row to the bronze Delta table.

Target: <catalog>.bronze.countries_raw
"""

import argparse
import json

import requests
from databricks.sdk.runtime import spark
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StringType, StructField, StructType

_BRONZE_SCHEMA = StructType([
    StructField("raw_json", StringType(), nullable=False),
])


def ingest_countries(catalog: str, source_api_url: str) -> None:
    response = requests.get(source_api_url, timeout=30)
    response.raise_for_status()

    rows = [(json.dumps(country),) for country in response.json()]

    df = (
        spark.createDataFrame(rows, schema=_BRONZE_SCHEMA)
        .withColumn("source_url", lit(source_api_url))
        .withColumn("ingested_at", current_timestamp())
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.bronze")
    df.write.format("delta").mode("overwrite").saveAsTable(f"`{catalog}`.bronze.countries_raw")

    print(f"Ingested {len(rows)} countries → `{catalog}`.bronze.countries_raw")


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest REST Countries data to bronze")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    parser.add_argument(
        "--source-api-url",
        # ?fields= is required by the v3.1 API — omitting it returns HTTP 400.
        # Extend this list when new silver/gold columns are needed.
        default="https://restcountries.com/v3.1/all?fields=name,cca2,cca3,region,subregion,population,area,landlocked,unMember,capital,continents,flags",
        help="REST Countries API endpoint (must include ?fields=)",
    )
    args = parser.parse_args()
    ingest_countries(args.catalog, args.source_api_url)


if __name__ == "__main__":
    main()
