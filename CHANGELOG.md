# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] — 2026-05-06

### Added

- `src/atlas_stream/etl/schema.py` — explicit `COUNTRY_ROW_SCHEMA` (`StructType`) replacing brittle single-row `schema_of_json` inference; versioned via `SCHEMA_VERSION = 1`
- `fixtures/sample_countries.json` — 3-country static fixture (NL, DE, TD) used by unit tests
- `.github/workflows/ci.yml` — GitHub Actions CI: lint with ruff, pure-Python tests without credentials, wheel build, `bundle validate`
- `Makefile` — developer task runner (`lint`, `test`, `test-all`, `build`, `validate`, `deploy-dev`, `deploy-prod`, `run-dev`, `run-prod`)
- HTTP retry adapter on ingest session: 3 retries with exponential back-off for status codes 429, 500, 502, 503, 504
- `batch_id` UUID stamped on every bronze row for downstream deduplication
- `source_ingested_at` lineage column carried from bronze through silver into all gold tables
- Response validation in `ingest.py`: rejects non-list, empty, or missing-field API responses before writing to bronze
- 7-check data quality verification in `verify.py`: table existence + row count, silver row count within expected range, cca2 uniqueness, null region rate < 5%, no null population, no negative population
- `COMMENT ON TABLE` and `OPTIMIZE` after every Delta write (bronze, silver, all 6 gold tables)
- Job-level `tags`, `email_notifications` (on_failure), and `health` (max duration) in `atlas_stream_job.job.yml`
- Per-task `timeout_seconds` and `max_retries` / `min_retry_interval_millis` on all four tasks
- `sync.exclude` in `databricks.yml` to omit `__pycache__`, `.venv`, `dist`, `*.pyc`, etc.
- `notification_email` bundle variable (default `dbbruijn@gmail.com`) threaded into job failure alerts
- `pytest-cov`, `pytest-mock`, `responses` added as dev dependencies
- `[tool.pytest.ini_options]` and `[tool.coverage.run]` sections in `pyproject.toml`
- `spark` pytest marker registered in `conftest.py` and `pyproject.toml`

### Changed

- `ingest.py` — bronze writes changed from `mode("overwrite")` to `mode("append").option("mergeSchema","true")` (immutable raw layer; bronze now accumulates all historical batches)
- `ingest.py` — HTTP timeout changed from a single integer to a `(5, 30)` tuple (separate connect / read timeouts)
- `ingest.py` — replaced `print()` with `logging.getLogger(__name__)` structured logging throughout
- `bronze_to_silver.py` — JSON parsing now uses `COUNTRY_ROW_SCHEMA` via `from_json()` instead of single-row `schema_of_json`
- `bronze_to_silver.py` — processes only the latest bronze batch (`MAX(ingested_at)` filter) instead of the full table
- `bronze_to_silver.py` — logs cca2 filter drop fraction; aborts if > 2% of rows are dropped
- `silver_to_gold.py` — `size_category` now maps `area IS NULL → "Unknown"` before the numeric thresholds (was silently classified as "Small")
- `silver_to_gold.py` — all gold writes now use `mode("overwrite").option("overwriteSchema","true")`
- `tests/test_countries_etl.py` — expanded from 12 to 19 tests; 15 run without credentials; 4 Spark tests require Databricks Connect
- `pyproject.toml` — added `requests<3` upper bound; removed `databricks-dlt` dev dependency

### Fixed

- `verify.py` — population check changed from `population <= 0` to `population < 0` so the 5 uninhabited territories (Antarctica, Bouvet Island, etc.) with `population = 0` are no longer incorrectly flagged as data quality failures

## [0.2.0] — 2026-05-06

### Added

- `gold.countries` — enriched country-level table with `population_density` (people / km²) and `size_category` bucket (`Very Large` / `Large` / `Medium` / `Small`)
- `gold.countries_by_continent` — continent-level rollup (collapses subregions into a single row per continent)
- `gold.population_tiers` — countries bucketed by population size (`Large ≥100M` / `Medium 10M–100M` / `Small 1M–10M` / `Micro <1M`)
- `gold.landlocked_vs_coastal` — landlocked vs coastal country counts and population totals per region
- `gold.un_membership_summary` — UN member vs non-member counts and population totals per region

### Changed

- `silver_to_gold.py` — rewritten to produce six gold tables (was one); `_write()` helper introduced to DRY table writes
- `verify.py` — updated `_TABLES` list to assert all eight medallion tables non-empty (was three)
- `tests/test_countries_etl.py` — updated verify and schema tests to cover the expanded table set (12 tests total)
- Architecture diagram and SQL examples in `README.md` updated to reflect six gold tables

### Fixed

- Removed `cache()` / `unpersist()` calls that are not supported on Databricks serverless compute
- `_write()` in `silver_to_gold.py` and post-write logging in `bronze_to_silver.py` now read row counts from the already-written Delta table instead of re-executing the full transformation plan (double-compute antipattern)

## [0.1.0] — 2026-05-06

### Added

- Four-task serverless ETL job (`atlas_stream_job`) — ingest → bronze → silver → gold → verify
- `ingest.py` — fetches all countries from REST Countries API v3.1, writes raw JSON to `bronze.countries_raw`
- `bronze_to_silver.py` — parses JSON using serverless-safe `from_json` + `schema_of_json` pattern, writes typed rows to `silver.countries`
- `silver_to_gold.py` — aggregates by `region` / `subregion` to produce `gold.countries_by_region`
- `verify.py` — asserts all three medallion tables are non-empty; fails the job on empty tables
- `tests/test_countries_etl.py` — 11 unit tests covering argument parsing, JSON serialisation, HTTP error handling, schema validation, and the verify table list
- `tests/conftest.py` — lazy Databricks Connect fixture (explicit `atlas_stream_dev` profile; pure-Python tests always run, Spark tests skip gracefully when auth is unavailable)
- `databricks.yml` — four variables (`catalog`, `schema`, `source_api_url`, `schedule_pause_status`) with sane per-target defaults; `dev` target prefixes resources and pauses schedules automatically

### Changed

- `pyproject.toml` — removed legacy `main` entry point; updated ruff `per-file-ignores` to remove deleted `atlas_stream_etl` directory reference

### Removed

- `src/atlas_stream/main.py` — legacy entry point (unused)
- `src/atlas_stream/taxis.py` — NYC taxi sample code (unrelated to countries pipeline)
- `src/sample_notebook.ipynb` — notebook task example (unused)
- `src/atlas_stream_etl/` — DLT sample pipeline directory (trips/zones transformations)
- `resources/atlas_stream_etl.pipeline.yml` — DLT pipeline resource definition
- `tests/sample_taxis_test.py` — tests for deleted sample code

## [0.0.1] — 2026-05-06

### Added

- Initial Databricks Asset Bundle scaffold for `atlas_stream` country pipeline
- DLT pipeline (`atlas_stream_etl`) with sample trips and zones transformations
- Orchestration job (`atlas_stream_job`) — notebook task → wheel task → pipeline refresh
- Python wheel package (`atlas_stream`) with `main` entry point
- `dev` and `prod` deployment targets pointing to `dbc-acdc353e-0e47.cloud.databricks.com`
- pytest fixture infrastructure with `DatabricksSession` and fixture-file loader
- VS Code / Cursor workspace settings and recommended extensions
- `uv` for dependency management and wheel builds
- `ruff` for linting (line-length 120, DLT runtime-global suppressions)
