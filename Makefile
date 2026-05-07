# Development task runner for atlas_stream.
# Targets run in dependency order: build depends on lint + test.

.PHONY: lint test build validate deploy-dev

lint:
	uv run ruff check src/ tests/

test:
	uv run pytest -v -m "not spark"

test-all:
	uv run pytest -v

build: lint test
	uv build --wheel

validate: build
	databricks bundle validate --target dev

deploy-dev: validate
	databricks bundle deploy --target dev

deploy-prod: build
	databricks bundle deploy --target prod

run-dev:
	databricks bundle run atlas_stream_job --target dev

run-prod:
	databricks bundle run atlas_stream_job --target prod
