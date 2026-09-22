# Desarrollo

## Entorno Python reproducible

`uv` 0.12.x es la única herramienta soportada para administrar Python y dependencias. `uv.lock` fija la resolución y el extra `dev` instala únicamente las herramientas necesarias para desarrollo y CI.

```bash
make install
```

`make install` equivale a:

```bash
uv sync --locked --python 3.12 --extra dev
```

## Calidad local

```bash
make format          # aplica formato
make format-check    # comprueba formato sin editar
make lint
make test
make check-tracked   # falla si Git rastrea rutas prohibidas
make quality         # reproduce los controles de CI
```

Los equivalentes sin Make son:

```bash
uv run --locked --extra dev ruff format --check .
uv run --locked --extra dev ruff check .
uv run --locked --extra dev pytest
uv run --locked --extra dev python scripts/check_tracked_files.py
```

No es necesario activar `.venv`. GitHub Actions instala `uv`, sincroniza el mismo lockfile y ejecuta `make quality` sin secretos, AWS, datos reales, Airflow ni MLflow.

## Estructura del repositorio

```text
src/observatorio_secop/  límites de ingesta, procesamiento, scoring y API
dags/                    DAGs de Airflow futuros
dbt/                     modelos analíticos futuros
tests/                   pruebas y controles de plataforma
infra/                   imágenes locales e infraestructura futura
scripts/                 comandos operativos y de seguridad
docs/                    documentación pública
powerbi/                 documentación BI; nunca archivos .pbix
.github/workflows/       integración continua
```

Los datos, modelos, `mlruns/`, `.env`, cachés y archivos `.pbix` no se versionan. El control `make check-tracked` falla si una ruta prohibida llegó al índice de Git.

## Evidencia antes de un commit

Registra los resultados, nunca credenciales ni el contenido completo de `.env`:

```bash
make quality
make compose-config
make up
make health
make up-airflow
make up-mlflow
make ps
git status --short
git diff --check
git check-ignore -v .env data/example.parquet models/example.bin mlruns/example report.pbix
```

Los controles automatizados también cubren formato inválido, lint inválido, una prueba fallida, Docker no disponible, variables obligatorias ausentes y archivos prohibidos ya rastreados.

## Dependencias y licencias

Las capacidades futuras están separadas en los extras `data`, `orchestration`, `ml` y `api`; no se instalan en el flujo mínimo de CI.

| Componente ejecutado en US-00 | Licencia upstream revisada |
|---|---|
| uv | MIT o Apache License 2.0 |
| Hatchling, pytest, PyYAML y Ruff | MIT |
| PostgreSQL | PostgreSQL License |
| Apache Airflow | Apache License 2.0 |
| MLflow | Apache License 2.0 |
| psycopg2-binary | LGPL con excepciones documentadas por el proyecto |

Cada componente conserva su licencia upstream; el proyecto no incorpora su código fuente ni decide todavía la licencia final del producto.
