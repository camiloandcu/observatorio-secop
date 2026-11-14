# Desarrollo y verificación

Instala, comprueba y prepara cambios desde la raíz del repositorio. La receta de integración usa una base dedicada y desechable.

## Desarrollo

### Entorno Python reproducible

`uv` 0.12.x es la única herramienta soportada para administrar Python y dependencias. `uv.lock` fija la resolución; los extras `dev`, `data` y `warehouse` instalan las herramientas de calidad, PySpark, Pandera, dbt y el cliente PostgreSQL con sus dependencias reproducibles.

```bash
make install
```

`make install` equivale a:

```bash
uv sync --locked --python 3.12 --extra dev --extra data --extra warehouse
```

### Calidad local

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

No es necesario activar `.venv`. GitHub Actions instala los runtimes y ejecuta `make ci` sobre PostgreSQL desechable, además de escaneos independientes. Consulta [integración continua y seguridad](#integración-continua-y-seguridad) para reproducir todos los controles, revisar omisiones críticas y entender los límites medidos.

### Integración Silver real → PostgreSQL

Las pruebas offline incluyen publicación real con Spark y verificación de su Parquet. Las pruebas relacionales requieren `WAREHOUSE_TEST_DSN` y se omiten si no está definido; esa omisión no prueba la carga PostgreSQL.

Usa la receta única de [integración estricta](#reproducir-integración-estricta) de esta guía. Algunas pruebas eliminan schemas: nunca uses una base de trabajo. Para una comprobación focalizada, conserva el mismo DSN desechable y selecciona `tests/processing_silver/test_warehouse_boundary.py` y `tests/warehouse`. La suite incluye restauración A → B → A, conflictos, concurrencia y preservación transaccional.

La suite de [identidad de plataforma](../referencia/04_identidad.md) usa datos sintéticos y el mismo
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

### Actualizar dependencias

Consulta [actualización compatible de dependencias](../referencia/06_dependencias.md) para
verificar el grafo conjunto, las imágenes opcionales y migraciones de estado antes
de actualizar servicios existentes. Los hallazgos de los escáneres siguen bloqueando CI. La migración Airflow 3 exige
el [ensayo de copia y restauración](03_operacion.md#actualizar-o-recuperar-metadatos-airflow)
antes de actualizar una base existente.

### Contrato de la fuente SECOP II

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

### Estructura del repositorio

```text
src/observatorio_secop/  límites de ingesta, procesamiento, scoring y API
dags/                    DAG de adquisición y publicación analítica
dbt/                     modelo estrella, pruebas y catálogo analítico
tests/                   pruebas y controles de plataforma
infra/                   imágenes locales e infraestructura futura
scripts/                 comandos operativos y de seguridad
docs/                    documentación pública
bi/                      constructor y medidas Power BI; nunca archivos .pbix
.github/workflows/       integración continua
```

Los datos, modelos, `mlruns/`, `.env`, cachés y archivos `.pbix` no se versionan. El control `make check-tracked` falla si una ruta prohibida llegó al índice de Git.

### Evidencia antes de un commit

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

## Integración continua y seguridad

El workflow ejecuta calidad e integración real, dos controles de seguridad
independientes y un job de runtimes opcionales. Un resultado aprobado exige todos: las pruebas offline sin PostgreSQL
no acreditan integración. El tipado actual cubre únicamente
`src/observatorio_secop/warehouse` y `src/observatorio_secop/publication`; formato, lint y pruebas tienen alcance más amplio.

### Reproducir integración estricta

Instala uv 0.12.0, Python 3.12 y Java 17. `make install` usa el lock y los extras
`dev`, `data` y `warehouse`. El workflow usa Ubuntu 24.04 y PostgreSQL 16.4, con
acciones fijadas por SHA e imagen por digest. Los parches efectivos quedan en la
evidencia; los cambios de versiones requieren repetir los controles afectados.

Algunas pruebas eliminan schemas completos. Crea una base **dedicada y desechable**,
sin volumen de trabajo ni datos reales. Este contenedor expone solo el loopback local:

```bash
make install
docker run --detach --rm --name secop-ci-test-postgres \
  --publish 127.0.0.1:55432:5432 \
  --env POSTGRES_PASSWORD=secop_ci_test_only \
  --env POSTGRES_DB=secop_ci_local postgres:16.4-alpine
docker exec secop-ci-test-postgres pg_isready -U postgres -d secop_ci_local
export WAREHOUSE_TEST_DSN='host=127.0.0.1 port=55432 dbname=secop_ci_local user=postgres password=secop_ci_test_only'
export CI_DISPOSABLE_DB_HOST=127.0.0.1
export CI_DISPOSABLE_DB_PORT=55432
export CI_DISPOSABLE_DB_NAME=secop_ci_local
export SPARK_LOCAL_IP=127.0.0.1
make ci-preflight
make ci
```

Espera la respuesta saludable de `pg_isready`. El preflight exige destino explícito
coincidente, nombre `secop_ci_*`, Java 17 y runtimes de Spark/dbt. No infiere la base
ni usa la configuración de trabajo cuando falta el DSN. El nombre por sí solo no
hace desechable una base: el operador debe crear el contenedor dedicado mostrado.
La contraseña mostrada es un valor de fixture para el servicio efímero aislado;
no se reutiliza para cuentas ni servidores compartidos. El contrato de la CLI exige
una contraseña no vacía, incluso cuando una conexión local permitiría otra autenticación.

El runner ejecuta `make quality` serialmente. Las pruebas construyen Bronze sintético,
Silver con Spark real, verifican su inventario, cargan PostgreSQL y ejecutan dbt.
Comprueban precisión temporal, rutas, dinero decimal, identidad, población,
reconstrucción, restauración A → B → A y fallos que conservan la versión válida.
La restauración incluye reconstruir dbt y conciliar filas, monto y versión de A.
La publicación añade construcción aislada y validada, roles reales de lectura,
activación/limpieza concurrentes, lectura entre conexiones y recuperación de
candidatos. Los tests de validación/lectores de publicación también son una
familia crítica: no pueden omitirse aunque se encuentren fuera del directorio
de integración relacional.
No ejecutes dos suites relacionales contra la misma base ni uses ejecución paralela
para estas fixtures: comparten schemas y salidas dbt.

Al terminar, conserva solo los reportes necesarios y detén el contenedor dedicado:

```bash
unset WAREHOUSE_TEST_DSN CI_DISPOSABLE_DB_HOST CI_DISPOSABLE_DB_PORT CI_DISPOSABLE_DB_NAME
docker stop secop-ci-test-postgres
```

`make quality` sigue siendo útil sin base para desarrollo offline; sus skips se
muestran, pero no sustituyen `make ci` para verificar integración.

### Inventario y evidencia de ejecución

`config/ci_integration.json` fija los casos críticos revisados y las familias cuya
colección actual también es obligatoria. Cuando cambia un identificador, revisa el
cambio semántico y actualiza el inventario; no lo regeneres automáticamente para
hacer desaparecer un caso fallido. Las nuevas pruebas de familias críticas también
se exigen aunque no estuvieran en la lista inicial.

`make ci` produce exclusivamente estos reportes subibles:

| Archivo | Contenido |
|---|---|
| `reports/ci/outcomes.json` | Identificadores recolectados/deseleccionados y resultados/duración por fase |
| `reports/ci/summary.json` | Conteos por familia, faltantes, resultado del gate y versiones |
| `reports/ci/resources.json` | Duración, SHA, CPU/RAM/disco y muestras de memoria/base |

No contienen mensajes completos de excepción, DSN ni perfiles. Los motivos de
omisión se expresan mediante categorías saneadas. Una prueba crítica ausente,
deseleccionada, omitida, xfail/xpass, fallida o con error hace fallar el gate.
Un reporte vacío, malformado o incompleto también falla. Se conserva el error
original de pytest; producir diagnósticos no lo convierte en éxito.

Para revisar un reporte ya generado sin repetir la suite:

```bash
uv run --locked --extra dev --extra data --extra warehouse \
  python scripts/ci_integration.py gate --report reports/ci/outcomes.json
```

### Escaneos de seguridad

Los controles requieren acceso a las descargas de herramientas y al servicio público
de advisories, sin una cuenta nueva ni tokens cloud. Los datos de prueba permanecen
sintéticos y no se consulta SECOP. Ejecuta:

```bash
make security-secrets
make security-dependencies
# ambos, serialmente:
make security
```

Gitleaks analiza el historial Git obtenido, incluidos secretos eliminados del HEAD,
con salida redactada y versión/descarga verificadas. Un checkout superficial no
acredita cobertura histórica; CI obtiene el historial completo. El escaneo se limita
a Git y no recorre indiscriminadamente archivos privados locales no versionados.

pip-audit analiza una exportación locked de **todos** los extras declarados:
`dev`, `data`, `warehouse`, `orchestration`, `ml` y `api`. No instala esos servicios
para auditarlos. El reporte declara intérprete/plataforma, marcadores y exclusiones;
un resultado no demuestra seguridad de otros ecosistemas, plataformas o fechas.
Las herramientas están fijadas en la configuración y el lock de auditoría separado.

Los códigos de salida distinguen éxito, hallazgos y fallo de herramienta/política.
Hallazgos sin excepción aprobada, historial incompleto, errores de descarga,
exportación o advisories bloquean; no se convierten en “sin vulnerabilidades”.
Los reportes permitidos son `reports/security/secrets.json` y
`reports/security/dependencies.json`, con metadatos y diagnósticos saneados.
No publiques los archivos temporales sin redactar.

Las excepciones son explícitas y requieren revisión humana. Para vulnerabilidades:
advisory y alias, paquete/versión, aplicabilidad, mitigación, responsable, aprobación y
vencimiento. Deben coincidir exactamente; una excepción vencida o de otro paquete
no permite el hallazgo. Para secretos solo se admiten falsos positivos identificados
por fingerprint/regla/ruta y justificados; un secreto usable necesita remoción y
rotación en su ámbito correspondiente. No se crea una baseline automática ni se
actualizan dependencias como efecto del escaneo. Una migración incompatible requiere
revisar su alcance antes de ejecutarla.

### Permisos y privacidad

Los jobs usan `contents: read`, checkout sin persistencia de credenciales, runners
efímeros y acciones inmutables. No se usa ejecución privilegiada de código de PR,
OIDC ni tokens de escritura. El servicio PostgreSQL es propio del job.

Las cachés del workflow están desactivadas. Solo se sube la allowlist de reportes
saneados anterior, con retención de siete días. Quedan fuera variables locales,
perfiles, datos, Parquet, rechazos, dumps, salidas completas de dbt, workspace y
planificación privada. Los controles del índice también rechazan archivos privados
forzados mediante `git add -f`; `.gitignore` no basta por sí solo.

### Presupuesto y diagnóstico

Los límites iniciales son 30 minutos para calidad/integración y 10 minutos por
escáner. Se contrastan con una ejecución completa real; un límite configurado no
es una medición. El entorno nuevo de Actions con cachés desactivadas permite medir
la instalación y la suite desde un runner frío. La evidencia de cada ejecución
identifica SHA, versiones y tamaño lógico de fixtures.

El runner registra preflight/calidad y duraciones de fases pytest; el log de Actions
muestra además instalación y pasos de seguridad. Muestrea cada segundo RSS por
grupos Python/JVM/PostgreSQL visibles en `/proc` y `pg_database_size` de la base
exclusiva, además del espacio de workspace/disco. Es una observación del entorno:
RSS puede duplicar páginas compartidas, perder picos breves e incluir otros procesos;
PostgreSQL en otro namespace puede no ser visible en RSS. El tamaño de la base no
incluye WAL ni capas del contenedor. La memoria disponible declarada describe el
host visible, no garantiza una cuota de memoria del job.

Si falla preflight, corrige runtimes/destino y no ejecutes fixtures destructivas.
Si falla el gate, consulta los identificadores faltantes y resultados; no silencies
skips. Si hay timeout, registra la etapa, recursos y datos conocidos antes de
revisar el margen o la topología. Si falla un escáner, distingue un hallazgo de un
error de herramienta/red y repite solo después del diagnóstico o corrección.
No amplíes límites ni agregues excepciones silenciosamente.

El build y smoke test de imagen API se incorporarán con el servicio API; este
workflow no presenta los marcadores de ese servicio como una API funcional.

### Compatibilidad de runtimes opcionales

El job `optional-runtimes` instala los seis extras del lock y verifica APIs mixtas,
migración/restauración Airflow con metadatos sintéticos, tarea por API de ejecución,
autenticación y el lifecycle del Compose real con PostgreSQL MLflow. Usa únicamente
proyectos, bases y contenedores desechables; los reportes saneados se conservan siete
días. La política de 25 minutos cubre instalación y descargas sin caché; revisar
duración real y margen con cada migración. Consulta comandos, alcance y límites en
[mantenimiento de dependencias](../referencia/06_dependencias.md). No se publican backups,
logs de servicios, credenciales ni archivos de configuración privados.


### Replay real del pipeline

`make pipeline-e2e` construye la imagen y ejecuta el gate sobre un proyecto Compose,
metadata Airflow y warehouse desechables propios. El job `pipeline-e2e` espera los
jobs de calidad y runtimes opcionales para evitar su superposición pesada. Comprueba
el scheduler y Execution API con las siete tareas reales; el transporte fuente es
sintético, mientras Spark, PostgreSQL, dbt y publicación ejecutan sus productores.
No necesita Socrata ni AWS durante el replay preparado.

El inventario obligatorio incluye reconstrucción fría con resultados conocidos,
corte después del commit de adquisición con reintento del mismo grant y candidata
rechazada que conserva la publicación válida. Importar el DAG, pasar health checks
o dejar casos/tareas omitidos no satisface ese inventario. El gate conserva límites
finitos, inspecciona el candidato fallido antes de descartarlo explícitamente y
comprueba la eliminación de sus recursos propios.

Solo `reports/orchestration/e2e.json` se publica como evidencia saneada, con retención
de siete días. Identifica imagen/código/lock/fixtures, estados e intentos por tarea,
duraciones, resultados conocidos, alcance de la medición y cleanup. Los snapshots
de recursos observados no se presentan como picos garantizados. Diagnósticos, logs,
configuración, credenciales y artefactos detallados permanecen privados. Revisa el
resultado real y sus limitaciones; tener el gate configurado no prueba su ejecución.
El [runbook](04_pipeline.md) explica la operación manual y recuperación.
