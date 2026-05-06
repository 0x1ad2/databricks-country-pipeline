# atlas_stream — Databricks Country Pipeline

A [Databricks Asset Bundle](https://docs.databricks.com/dev-tools/bundles/index.html) for the `atlas_stream` country data pipeline. It ships a Delta Live Tables (DLT) pipeline, a scheduled orchestration job, and a deployable Python wheel — all managed as code.

## Project structure

```
databricks.yml                  # Bundle definition — targets, variables, workspace host
pyproject.toml                  # Python package & tooling config (uv, ruff, pytest)
resources/
  atlas_stream_etl.pipeline.yml # DLT pipeline resource definition
  atlas_stream_job.job.yml      # Orchestration job (notebook + wheel + pipeline refresh)
src/
  atlas_stream/                 # Shared Python package (deployed as a wheel)
    main.py                     # Entry point for the python_wheel_task
    taxis.py                    # Example data access layer
  atlas_stream_etl/
    transformations/            # DLT transformation files (one dataset per file)
    explorations/               # Ad-hoc notebooks (git-ignored, not deployed)
  sample_notebook.ipynb         # Notebook task example
tests/
  conftest.py                   # pytest fixtures — DatabricksSession, fixture loader
  sample_taxis_test.py          # Unit tests for the shared package
fixtures/                       # Static test data (JSON / CSV)
```

## Prerequisites

| Tool                                                                                  | Purpose                       |
| ------------------------------------------------------------------------------------- | ----------------------------- |
| [uv](https://docs.astral.sh/uv/getting-started/installation/)                         | Dependency management & build |
| [Databricks CLI](https://docs.databricks.com/dev-tools/cli/databricks-cli.html)       | Bundle deploy / run           |
| [Databricks VS Code extension](https://docs.databricks.com/dev-tools/vscode-ext.html) | IDE integration (optional)    |

## Getting started

### 1 — Install dependencies

```bash
uv sync --dev
```

### 2 — Authenticate

```bash
databricks configure
```

### 3 — Validate the bundle

```bash
databricks bundle validate
```

## Development workflow

### Build the Python wheel

```bash
uv build --wheel
```

### Run the linter

```bash
uv run ruff check src/ tests/
```

### Run tests

Tests require an active Databricks connection (uses `databricks-connect`):

```bash
uv run pytest
```

## Deployment

### Deploy to dev (default)

```bash
databricks bundle deploy
```

Dev deployments are prefixed with `[dev <your_name>]` and all schedules are paused automatically.

### Deploy to production

```bash
databricks bundle deploy --target prod
```

### Run a job or pipeline

```bash
# Trigger the orchestration job
databricks bundle run atlas_stream_job

# Run only the DLT pipeline
databricks bundle run atlas_stream_etl

# Run a single DLT transformation
databricks bundle run atlas_stream_etl --select sample_trips_atlas_stream
```

## Adding transformations

Create a new `.py` file under `src/atlas_stream_etl/transformations/`. The pipeline glob picks it up automatically:

```python
from pyspark import pipelines as dp

@dp.table
def my_new_table():
    return spark.read.table("source_catalog.source_schema.source_table")
```

## Variables

| Variable  | dev default       | prod default |
| --------- | ----------------- | ------------ |
| `catalog` | `workspace`       | `workspace`  |
| `schema`  | `<your_username>` | `prod`       |

Override on deploy: `databricks bundle deploy --var catalog=my_catalog`

## Workspace

[https://dbc-acdc353e-0e47.cloud.databricks.com](https://dbc-acdc353e-0e47.cloud.databricks.com)

4. To run a job or pipeline, use the "run" command:

   ```
   $ databricks bundle run
   ```

5. Finally, to run tests locally, use `pytest`:
   ```
   $ uv run pytest
   ```
