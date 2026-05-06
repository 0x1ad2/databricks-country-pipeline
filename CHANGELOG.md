# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
