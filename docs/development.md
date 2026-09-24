# Desarrollo

## Entorno Python reproducible

`uv` 0.12.x es la única herramienta soportada para administrar Python y dependencias. `uv.lock` fija la resolución; los extras `dev` y `data` instalan las herramientas de calidad, PySpark, Pandera y sus dependencias reproducibles.

```bash
make install
```

`make install` equivale a:

```bash
uv sync --locked --python 3.12 --extra dev --extra data
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
uv run --locked --extra dev --extra data ruff format --check .
uv run --locked --extra dev --extra data ruff check .
uv run --locked --extra dev --extra data pytest
uv run --locked --extra dev --extra data python scripts/check_tracked_files.py
```

No es necesario activar `.venv`. GitHub Actions instala `uv`, sincroniza el mismo lockfile y ejecuta `make quality` sin necesidad de tener la arquitectura montada. 

## Contrato de la fuente SECOP II

La validación habitual es completamente offline y utiliza una fixture pequeña:

```bash
make validate-source
make test
```

La observación de la fuente es una operación manual separada de CI:

```bash
make check-source-live  # verifica columnas críticas con una muestra de 10 filas
make profile-source     # vuelve a generar contrato, fixture y reporte para revisión
```

`make profile-source` consulta únicamente metadatos, agregaciones y muestras con límites configurados en `config/secop_source.yaml`. No necesita token, no descarga el histórico nacional y conserva los artefactos vigentes si la fuente falla.

Después de regenerar, revisa juntos:

- `contracts/secop_source.yaml`, consumible por código;
- `tests/fixtures/secop_contracts.json`, con una fila segura por municipio del Valle de Aburrá;
- `docs/data-source-profile.md`, resumen de la evidencia y sus limitaciones.

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
