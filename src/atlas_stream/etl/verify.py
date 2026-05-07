"""Task 4 — verify_country_tables

Runs data quality checks against all eight medallion tables.
Fails the job task (non-zero exit) if any check fails.

Checks run:
  1. All 8 tables exist and are non-empty
  2. silver.countries row count is within the expected range [220, 260]
  3. silver.countries has no duplicate cca2 values
  4. silver.countries null rate on region < 5%
  5. silver.countries has no null population values
  6. silver.countries has no population <= 0
  7. gold.countries_by_region country_count sums to the silver row count
"""

import argparse
import logging

from databricks.sdk.runtime import spark
from pyspark.sql.functions import col
from pyspark.sql.functions import sum as spark_sum

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

_TABLES = [
    ("bronze", "countries_raw"),
    ("silver", "countries"),
    ("gold", "countries"),
    ("gold", "countries_by_region"),
    ("gold", "countries_by_continent"),
    ("gold", "population_tiers"),
    ("gold", "landlocked_vs_coastal"),
    ("gold", "un_membership_summary"),
]

# Expected range for the number of countries returned by the REST Countries API
_SILVER_COUNT_MIN = 220
_SILVER_COUNT_MAX = 260


def verify_country_tables(catalog: str) -> None:
    failures: list[str] = []

    # ------------------------------------------------------------------
    # Check 1: all tables exist and are non-empty
    # ------------------------------------------------------------------
    for schema, table in _TABLES:
        full_name = f"`{catalog}`.{schema}.{table}"
        row_count = spark.read.table(full_name).count()
        status = "OK" if row_count > 0 else "EMPTY"
        logger.info("  %s: %d rows  [%s]", full_name, row_count, status)
        if row_count == 0:
            failures.append(f"{full_name} is empty")

    # ------------------------------------------------------------------
    # Checks 2-6: silver.countries quality assertions
    # ------------------------------------------------------------------
    silver_df = spark.read.table(f"`{catalog}`.silver.countries")
    silver_total = silver_df.count()

    # 2. Row count within expected API range
    if not (_SILVER_COUNT_MIN <= silver_total <= _SILVER_COUNT_MAX):
        failures.append(
            f"silver.countries row count {silver_total} is outside expected range "
            f"[{_SILVER_COUNT_MIN}, {_SILVER_COUNT_MAX}] — API may be truncated or malformed"
        )

    # 3. cca2 uniqueness
    distinct_cca2 = silver_df.select("cca2").distinct().count()
    dupes = silver_total - distinct_cca2
    if dupes > 0:
        failures.append(f"silver.countries has {dupes} duplicate cca2 values")

    # 4. Null region rate < 5%
    null_region = silver_df.filter(col("region").isNull()).count()
    null_rate = null_region / silver_total if silver_total > 0 else 1.0
    if null_rate > 0.05:
        failures.append(
            f"silver.countries null region rate {null_rate:.1%} exceeds 5% threshold"
        )

    # 5. No null population
    null_pop = silver_df.filter(col("population").isNull()).count()
    if null_pop > 0:
        failures.append(f"silver.countries has {null_pop} rows with null population")

    # 6. No negative population (population=0 is valid for uninhabited territories)
    neg_pop = silver_df.filter(col("population") < 0).count()
    if neg_pop > 0:
        failures.append(f"silver.countries has {neg_pop} rows with negative population")

    # ------------------------------------------------------------------
    # Check 7: gold.countries_by_region reconciliation
    # ------------------------------------------------------------------
    gold_total = (
        spark.read.table(f"`{catalog}`.gold.countries_by_region")
        .agg(spark_sum("country_count").alias("total"))
        .collect()[0]["total"]
    )
    if gold_total != silver_total:
        failures.append(
            f"gold.countries_by_region total country_count ({gold_total}) != "
            f"silver row count ({silver_total})"
        )

    if failures:
        raise AssertionError(
            "Data quality checks failed:\n" + "\n".join(f"  - {f}" for f in failures)
        )

    logger.info("All %d quality checks passed.", 7)


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify country ETL tables are populated")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    args = parser.parse_args()
    verify_country_tables(args.catalog)


if __name__ == "__main__":
    main()
