# Desarrollo

## Entorno Python reproducible

`uv` 0.12.x es la única herramienta soportada para administrar Python y dependencias. `uv.lock` fija la resolución; los extras `dev`, `data` y `warehouse` instalan las herramientas de calidad, PySpark, Pandera, dbt y el cliente PostgreSQL con sus dependencias reproducibles.

```bash
make install
```

`make install` equivale a:

```bash
uv sync --locked --python 3.12 --extra dev --extra data --extra warehouse
```

## Calidad local

```bash
make format          # aplica formato
make format-check    # comprueba formato sin editar
make lint
make typecheck
make test
make check-tracked   # falla si Git rastrea rutas prohibidas
make quality         # controles locales; sin DSN puede omitir integración
make ci              # integración estricta: requiere base desechable y Java 17
make security        # escaneos de secretos e inventario de dependencias
```

Los equivalentes sin Make son:

```bash
uv run --locked --extra dev --extra data --extra warehouse ruff format --check .
uv run --locked --extra dev --extra data --extra warehouse ruff check .
uv run --locked --extra dev --extra data --extra warehouse mypy src/observatorio_secop/warehouse src/observatorio_secop/publication
uv run --locked --extra dev --extra data --extra warehouse pytest
uv run --locked --extra dev --extra data --extra warehouse python scripts/check_tracked_files.py
```

No es necesario activar `.venv`. GitHub Actions instala los runtimes y ejecuta `make ci` sobre PostgreSQL desechable, además de escaneos independientes. Consulta [integración continua y seguridad](continuous-integration.md) para reproducir todos los controles, revisar omisiones críticas y entender los límites medidos.

## Integración Silver real → PostgreSQL

Las pruebas offline incluyen publicación real con Spark y verificación de su Parquet. Las pruebas relacionales requieren `WAREHOUSE_TEST_DSN` y se omiten si no está definido; esa omisión no prueba la carga PostgreSQL.

Usa una base desechable dedicada: algunas pruebas eliminan schemas. Este ejemplo crea un contenedor de pruebas separado, sin volumen ni conexión a la base de trabajo:

```bash
docker run --detach --rm --name secop-test-postgres \
  --publish 127.0.0.1:55432:5432 \
  --env POSTGRES_USER=secop_test --env POSTGRES_PASSWORD=secop_test_only \
  --env POSTGRES_DB=secop_test postgres:16.4-alpine
docker exec secop-test-postgres pg_isready -U secop_test -d secop_test
export WAREHOUSE_TEST_DSN='host=127.0.0.1 port=55432 dbname=secop_test user=secop_test password=secop_test_only'
uv run --locked --extra dev --extra data --extra warehouse pytest \
  tests/processing_silver/test_warehouse_boundary.py \
  tests/warehouse
unset WAREHOUSE_TEST_DSN
docker stop secop-test-postgres
```

Espera a que `pg_isready` indique disponibilidad antes de ejecutar pytest. La integración parte de Bronze sintético, publica con el escritor real, verifica valores/precisión/nulos/linaje y carga dos veces: la segunda es no-op. También comprueba salida Spark INT96 anterior y preservación de evidencia. No necesita Socrata ni AWS. `make quality` con ese DSN ejecuta además el conjunto completo contra esa base de pruebas.

Las pruebas de warehouse demuestran además restauración A → B → A con comparación exacta y SHA-256 canónico, migración del esquema de auditoría anterior, conflictos históricos, fallos al registrar/restaurar estado y concurrencia observada en `pg_locks`. La guía de recuperación se ejercita con los entry points CLI de Silver y warehouse sobre bundles retenidos; no modifica datos de trabajo.

La suite de [identidad de plataforma](platform-identity.md) usa datos sintéticos y el mismo
DSN desechable. `tests/processing_silver/test_identity_pipeline.py` atraviesa Spark,
PostgreSQL y dbt para perfiles Bronze antiguos/nuevos, nombres cambiantes/ausentes,
códigos distintos con la misma etiqueta, recarga parcial, observaciones antiguas que
pierden la selección y restauración. `tests/warehouse/test_identity_refresh.py` verifica
límites, reanudación, IDs ausentes y checkpoint; los casos de migración comprueban DDL
antes de un no-op y rollback conjunto de esquema/filas/metadatos. Ejecuta esos casos sin
skips al aceptar una migración. El reporte agregado de la fuente está versionado; los
tests no consultan Socrata. El `pythonpath` de pytest incluye `tests` para reutilizar
los helpers de integración también al seleccionar un solo archivo.

La cobertura de inventario reproduce un Parquet adicional con conteo total coherente y comprueba su rechazo antes de decodificar/conectar, incluso en particiones nuevas y rutas ocultas. Verifica rutas canónicas, enlaces simbólicos, tamaño/hash, duplicados y las reglas explícitas de `_SUCCESS`/CRC. Observa las rutas que recibe Arrow y las lecturas de referencia INT96 para demostrar que solo se decodifica el conjunto verificado. La variante PostgreSQL compara exactamente filas, registro, historial y estado activo antes/después de cada carga inválida. La salida Spark real incluye años 2025/2026 y `unknown`, marcadores y sidecars, y conserva carga/repetición sin cambios en ambos formatos soportados. Estos casos se incluyen en los comandos anteriores; omitir la base deja sin verificar la preservación relacional.

## Actualizar dependencias

Consulta [actualización compatible de dependencias](dependency-maintenance.md) para
verificar el grafo conjunto, las imágenes opcionales y migraciones de estado antes
de actualizar servicios existentes. Los hallazgos de los escáneres siguen bloqueando CI. La migración Airflow 3 exige
el [ensayo de copia y restauración](operations.md#actualizar-o-recuperar-metadatos-airflow)
antes de actualizar una base existente.

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
dbt/                     modelo estrella, pruebas y catálogo analítico
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
