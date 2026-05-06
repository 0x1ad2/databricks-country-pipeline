"""pytest configuration: Databricks Connect fixtures for the atlas_stream ETL tests.

The ``spark`` fixture connects via Databricks Connect using the ``atlas_stream_dev``
profile.  Tests that declare ``spark`` as a parameter are automatically skipped when
authentication is unavailable (e.g. in CI without credentials).

All imports that touch the Databricks SDK are kept *inside* fixture bodies so that
pytest can collect and run pure-Python tests even without credentials.
"""

import csv
import json
import os
import pathlib

import pytest

# Resolve profile ambiguity: ensure the SDK uses the dev profile when running
# tests locally.  Must be set before any ETL module is imported because
# databricks.sdk.runtime initialises RemoteDbUtils (and hence Config) at
# module-load time.
os.environ.setdefault("DATABRICKS_CONFIG_PROFILE", "atlas_stream_dev")


@pytest.fixture()
def spark():
    """SparkSession via Databricks Connect (atlas_stream_dev profile).

    Skipped automatically when Databricks Connect is not installed or when
    authentication fails — allowing pure-Python tests to always run.

    Minimal example:
        def test_uses_spark(spark):
            df = spark.createDataFrame([(1,)], ["x"])
            assert df.count() == 1
    """
    try:
        from databricks.connect import DatabricksSession
    except ImportError:
        pytest.skip("databricks-connect not installed")

    try:
        return DatabricksSession.builder.profile("atlas_stream_dev").getOrCreate()
    except Exception as exc:
        pytest.skip(f"Databricks Connect unavailable: {exc}")


@pytest.fixture()
def load_fixture(spark):
    """Return a callable that loads JSON or CSV from the fixtures/ directory.

    Example usage:
        def test_using_fixture(load_fixture):
            data = load_fixture("my_data.json")
            assert data.count() >= 1
    """

    def _loader(filename: str):
        path = pathlib.Path(__file__).parent.parent / "fixtures" / filename
        suffix = path.suffix.lower()
        if suffix == ".json":
            rows = json.loads(path.read_text())
            return spark.createDataFrame(rows)
        if suffix == ".csv":
            with path.open(newline="") as f:
                rows = list(csv.DictReader(f))
            return spark.createDataFrame(rows)
        raise ValueError(f"Unsupported fixture type for: {filename}")

    return _loader
