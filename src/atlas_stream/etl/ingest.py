"""Task 1 — ingest_countries_to_bronze

Fetches all countries from the REST Countries API and appends each country's
raw JSON payload as a single row to the bronze Delta table.

Bronze is an immutable append-only landing zone. Every run adds a new batch
(identified by ``ingested_at`` and ``batch_id``) so previous raw data is
preserved for reprocessing and audit.

Target: <catalog>.bronze.countries_raw
"""

import argparse
import json
import logging
import uuid

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from databricks.sdk.runtime import spark
from pyspark.sql.functions import current_timestamp, lit
from pyspark.sql.types import StringType, StructField, StructType

from atlas_stream.etl.schema import REQUIRED_API_FIELDS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

_BRONZE_SCHEMA = StructType([
    StructField("raw_json", StringType(), nullable=False),
])

# (connect_timeout_s, read_timeout_s) — prevents indefinite hangs
_REQUEST_TIMEOUT = (5, 30)


def _build_session() -> requests.Session:
    """Return a requests Session with exponential-backoff retry on transient errors."""
    session = requests.Session()
    retry = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _validate_response(data: object, url: str) -> None:
    """Raise ValueError if the API response doesn't match the expected structure."""
    if not isinstance(data, list):
        raise ValueError(
            f"Expected JSON array from {url}, got {type(data).__name__}. "
            "API may have changed its response format."
        )
    if len(data) == 0:
        raise ValueError(f"REST Countries API returned empty array from {url}")
    missing = REQUIRED_API_FIELDS - data[0].keys()
    if missing:
        raise ValueError(
            f"API response missing expected fields: {missing}. "
            "Check that the ?fields= query parameter includes all required fields."
        )


def ingest_countries(catalog: str, source_api_url: str) -> None:
    batch_id = str(uuid.uuid4())
    logger.info("Starting ingest batch %s from %s", batch_id, source_api_url)

    session = _build_session()
    response = session.get(source_api_url, timeout=_REQUEST_TIMEOUT)
    response.raise_for_status()

    data = response.json()
    _validate_response(data, source_api_url)

    rows = [(json.dumps(country),) for country in data]
    logger.info("Fetched %d countries from API", len(rows))

    df = (
        spark.createDataFrame(rows, schema=_BRONZE_SCHEMA)
        .withColumn("source_url", lit(source_api_url))
        .withColumn("batch_id", lit(batch_id))
        .withColumn("ingested_at", current_timestamp())
    )

    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.bronze")
    # Append — bronze is immutable; never overwrite raw history.
    # mergeSchema handles first-run schema evolution (e.g. adding batch_id).
    df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(
        f"`{catalog}`.bronze.countries_raw"
    )

    logger.info(
        "Appended %d rows (batch_id=%s) \u2192 `%s`.bronze.countries_raw",
        len(rows), batch_id, catalog,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest REST Countries data to bronze")
    parser.add_argument("--catalog", required=True, help="Unity Catalog catalog name")
    parser.add_argument(
        "--source-api-url",
        # ?fields= is required by the v3.1 API — omitting it returns HTTP 400.
        # The API allows a maximum of 10 fields per request.
        default="https://restcountries.com/v3.1/all?fields=name,cca2,cca3,region,subregion,population,area,landlocked,unMember",
        help="REST Countries API endpoint (must include ?fields=, max 10 fields)",
    )
    args = parser.parse_args()
    ingest_countries(args.catalog, args.source_api_url)


if __name__ == "__main__":
    main()
