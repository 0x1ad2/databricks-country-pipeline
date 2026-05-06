# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
