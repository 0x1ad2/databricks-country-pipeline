"""Task 4 — verify_country_tables

Asserts that all three medallion tables exist and contain data.
Fails the job task (non-zero exit) if any table is empty.

Checks:
  <catalog>.bronze.countries_raw
  <catalog>.silver.countries
  <catalog>.gold.countries_by_region
"""

import argparse

from databricks.sdk.runtime import spark

_TABLES = [
    ("bronze", "countries_raw"),
    ("silver", "countries"),
    ("gold", "countries_by_region"),
]


def verify_country_tables(catalog: str) -> None:
    failures = []

    for schema, table in _TABLES:
        full_name = f"`{catalog}`.{schema}.{table}"
        row_count = spark.read.table(full_name).count()
        status = "OK" if row_count > 0 else "EMPTY"
        print(f"  {full_name}: {row_count} rows  [{status}]")
        if row_count == 0:
            failures.append(full_name)

    if failures:
        raise AssertionError(f"The following tables are empty after the ETL run: {failures}")

    print("All country tables verified successfully.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify country ETL tables are populated")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    args = parser.parse_args()
    verify_country_tables(args.catalog)


if __name__ == "__main__":
    main()
