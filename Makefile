UV ?= uv
UV_RUN := $(UV) run --locked --extra dev --extra data --extra warehouse
DBT_PROFILES_DIR ?= $(CURDIR)/.dbt
DBT := $(UV_RUN) dbt
DBT_OPTIONS := --project-dir dbt --profiles-dir $(DBT_PROFILES_DIR)

BRONZE_FROM ?= 2026-09-01T00:00:00Z
BRONZE_TO ?= 2026-09-02T00:00:00Z
BRONZE_MAX_ROWS ?= 100
BRONZE_PAGE_SIZE ?= 50

.PHONY: install format format-check lint typecheck test check-tracked quality profile-source \
	validate-source check-source-live ingest-bronze inspect-bronze clean-bronze-staging \
	build-silver inspect-silver clean-silver-staging \
	load-warehouse dbt-debug dbt-parse dbt-build dbt-test dbt-docs \
	check-path validate-env compose-config preflight up up-airflow up-mlflow health ps logs \
	down clean

install:
	$(UV) sync --locked --python 3.12 --extra dev --extra data --extra warehouse

format:
	$(UV_RUN) ruff format .

format-check:
	$(UV_RUN) ruff format --check .

lint:
	$(UV_RUN) ruff check .

typecheck:
	$(UV_RUN) mypy src/observatorio_secop/warehouse

test:
	$(UV_RUN) pytest

check-tracked:
	$(UV_RUN) python scripts/check_tracked_files.py

quality: format-check lint typecheck test check-tracked

profile-source:
	$(UV_RUN) secop-source-profile live --write

validate-source:
	$(UV_RUN) secop-source-profile validate

check-source-live:
	$(UV_RUN) secop-source-profile check-live

ingest-bronze:
	$(UV_RUN) secop-ingest run --from "$(BRONZE_FROM)" --to "$(BRONZE_TO)" \
		--max-rows "$(BRONZE_MAX_ROWS)" --page-size "$(BRONZE_PAGE_SIZE)"

inspect-bronze:
	$(UV_RUN) secop-ingest inspect

clean-bronze-staging:
	$(UV_RUN) secop-ingest clean-staging

build-silver:
	$(UV_RUN) secop-silver run $(SILVER_ARGS)

inspect-silver:
	$(UV_RUN) secop-silver inspect

clean-silver-staging:
	$(UV_RUN) secop-silver clean-staging

load-warehouse:
	$(UV_RUN) secop-warehouse load $(WAREHOUSE_ARGS)

dbt-debug:
	$(DBT) debug $(DBT_OPTIONS)

dbt-parse:
	$(DBT) parse $(DBT_OPTIONS)

dbt-build:
	$(DBT) build $(DBT_ARGS) $(DBT_OPTIONS)

dbt-test:
	$(DBT) test $(DBT_ARGS) $(DBT_OPTIONS)

dbt-docs:
	$(DBT) docs generate $(DBT_OPTIONS)

check-path:
	./scripts/local.sh check-path

validate-env:
	./scripts/local.sh validate-env

compose-config:
	./scripts/local.sh config

preflight:
	./scripts/local.sh preflight

up:
	./scripts/local.sh up

up-airflow:
	./scripts/local.sh up-airflow

up-mlflow:
	./scripts/local.sh up-mlflow

health:
	./scripts/local.sh health

ps:
	./scripts/local.sh ps

logs:
	./scripts/local.sh logs $(SERVICE)

down:
	./scripts/local.sh down

clean:
	CONFIRM_CLEAN=yes ./scripts/local.sh clean
