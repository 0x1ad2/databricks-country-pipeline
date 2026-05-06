# atlas_stream — Databricks Country Pipeline

A [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/index.html) that ingests country data from the [REST Countries API](https://restcountries.com), processes it through a Bronze → Silver → Gold medallion architecture, and schedules it as a daily serverless job on [Databricks Free Edition](https://www.databricks.com/product/freetrials).

## Architecture

```
REST Countries API
        │
        ▼
┌───────────────────┐     ┌───────────────────┐     ┌─────────────────────────┐
│  bronze           │────▶│  silver           │────▶│  gold                   │
│  countries_raw    │     │  countries        │     │  countries_by_region    │
│  (raw JSON rows)  │     │  (typed columns)  │     │  (aggregated analytics) │
└───────────────────┘     └───────────────────┘     └─────────────────────────┘
        │                         │                           │
        └─────────────────────────┴───────────────────────────┘
                                  │
                              verify task
                     (asserts all 3 tables non-empty)
```

All four tasks run on **serverless compute** (no cluster management needed).

## Project structure

```
databricks.yml                        # Bundle: targets, variables, artifact build
pyproject.toml                        # Python package, entry points, uv + ruff config
resources/
  atlas_stream_job.job.yml            # 4-task orchestration job definition
src/
  atlas_stream/
    etl/
      ingest.py                       # Task 1: REST API → bronze.countries_raw
      bronze_to_silver.py             # Task 2: JSON parse → silver.countries
      silver_to_gold.py               # Task 3: Aggregate → gold.countries_by_region
      verify.py                       # Task 4: Assert all tables non-empty
tests/
  conftest.py                         # pytest fixtures (DatabricksSession, fixture loader)
  test_countries_etl.py               # Unit tests for all ETL tasks
fixtures/                             # Static test data (JSON / CSV)
```

## Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| [uv](https://docs.astral.sh/uv/getting-started/installation/) | latest | Dependency management & wheel build |
| [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html) | ≥ 0.292 | Bundle deploy / run |
| Python | 3.10 – 3.12 | Runtime |

## Getting started

### 1 — Install dependencies

```bash
uv sync --dev
```

### 2 — Authenticate

Configure two profiles in `~/.databrickscfg`:

```bash
# Development workspace
databricks configure --profile atlas_stream_dev

# Production workspace
databricks configure --profile atlas_stream
```

### 3 — Validate the bundle

```bash
databricks bundle validate
```

### 4 — Deploy and run

```bash
# Deploy to dev (default)
databricks bundle deploy

# Run the ETL job manually
databricks bundle run atlas_stream_job
```

## Development workflow

```bash
# Lint
uv run ruff check src/ tests/

# Build wheel
uv build --wheel

# Run tests (pure-Python tests always run; Spark tests require Databricks Connect auth)
uv run pytest -v

# Validate bundle before deploy
databricks bundle validate
```

## Job tasks

| # | Task key | Entry point | Source → Target |
|---|----------|-------------|-----------------|
| 1 | `ingest_countries_to_bronze` | `ingest_countries` | REST API → `bronze.countries_raw` |
| 2 | `bronze_countries_to_silver` | `bronze_countries_to_silver` | `bronze.countries_raw` → `silver.countries` |
| 3 | `silver_countries_to_gold` | `silver_countries_to_gold` | `silver.countries` → `gold.countries_by_region` |
| 4 | `verify_country_tables` | `verify_country_tables` | Asserts all three tables non-empty |

Tasks run in sequence with `depends_on` dependencies. A failure in any task halts the chain.

## Variables

| Variable | Dev default | Prod default | Override flag |
|----------|-------------|--------------|---------------|
| `catalog` | `workspace` | `workspace` | `--var catalog=my_catalog` |
| `schema` | `<username>` | `prod` | `--var schema=my_schema` |
| `source_api_url` | REST Countries v3.1 (9 fields) | same | `--var source_api_url=...` |
| `schedule_pause_status` | `PAUSED` | `UNPAUSED` | automatic per target |

Override on deploy:

```bash
databricks bundle deploy --target prod --var catalog=my_catalog
```

## Deployment targets

### `dev` (default)
- All resource names prefixed `[dev <username>]`
- Job schedule is **PAUSED** (run manually with `bundle run`)
- Schema defaults to your Databricks username

### `prod`
- Production resource names (no prefix)
- Job schedule is **UNPAUSED** — runs daily
- Requires `atlas_stream` profile in `~/.databrickscfg`

```bash
databricks bundle deploy --target prod
databricks bundle run atlas_stream_job --target prod
```

## Querying the data

After the job completes, query the gold layer in a Databricks notebook or SQL editor:

```sql
-- Regional summary
SELECT region, subregion, country_count, total_population, total_area_km2, un_member_count
FROM workspace.gold.countries_by_region
ORDER BY total_population DESC;

-- All countries
SELECT name_common, cca2, region, subregion, population, area, un_member
FROM workspace.silver.countries
ORDER BY population DESC;
```

## Workspace

[https://dbc-acdc353e-0e47.cloud.databricks.com](https://dbc-acdc353e-0e47.cloud.databricks.com)
