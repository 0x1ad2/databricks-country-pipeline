import argparse
from databricks.sdk.runtime import spark
from atlas_stream import taxis


def main():
    # Process command-line arguments
    parser = argparse.ArgumentParser(
        description="Databricks job with catalog and schema parameters",
    )
    parser.add_argument("--catalog", required=True)
    parser.add_argument("--schema", required=True)
    parser.add_argument(
        "--source-api-url",
        default="https://restcountries.com/v3.1/all",
        help="REST Countries source API endpoint",
    )
    args = parser.parse_args()

    # Set the default catalog and schema
    spark.sql(f"USE CATALOG {args.catalog}")
    spark.sql(f"USE SCHEMA {args.schema}")

    # Example: just find all taxis from a sample catalog
    taxis.find_all_taxis().show(5)


if __name__ == "__main__":
    main()
