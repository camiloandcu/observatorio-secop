# Mantenimiento y migración de dependencias

Los seis extras Python se resuelven juntos y quedan fijados en `uv.lock`. El grafo
seleccionado usa Airflow 3.3.2, dbt-core 1.11.16, PyArrow 25.0.1, pytest 9.1.1,
setuptools 84.2.1, FastAPI 0.136.3 y SQLAlchemy 2.1.6. Conserva Python 3.12,
PySpark 3.5, dbt-postgres 1.9 y MLflow 3.17.2. Las imágenes predeterminadas
opcionales se declaran en `.env.example`.

La migración sustituye el perfil local Airflow 2: API server, LocalExecutor,
scheduler y DAG processor separado, con autenticación SimpleAuthManager explícita.
No es compatible con arrancar el Compose nuevo usando la imagen antigua. El
comando de arranque rechaza ese pin antes de iniciar servicios; actualiza el `.env`
operativo solo después de ensayar migración y recuperación sobre una copia.

## Revisar una actualización

Consulta advisories y compatibilidad de productores y consumidores. Actualiza los
paquetes afectados y las cohortes que deban cambiar juntas; no fuerces requisitos
fuera de los declarados por sus padres. Por ejemplo, MLflow limita PyArrow a una
versión anterior a 26 y Airflow restringe el rango de FastAPI; dbt 1.11 permite la
corrección sqlparse 0.6 que el rango anterior de dbt impedía.

```bash
uv lock --upgrade-package paquete_afectado
make security-dependencies
```

La auditoría exporta todos los extras, comprueba nombres/versiones e identifica
exclusiones reales de plataforma. No se eliminan extras ni se aceptan excepciones
para obtener un resultado verde. Dependencias retiradas por cambios legítimos de
upstream, como Flask-AppBuilder al pasar de Airflow 2 al perfil seleccionado de
Airflow 3, no equivalen a reducir el alcance auditado. El resultado y los conteos
pueden cambiar con el feed; repite la auditoría sobre el commit final.

## Verificar el entorno mixto

Instala los seis extras en un entorno independiente del usado habitualmente:

```bash
UV_PROJECT_ENVIRONMENT=/tmp/secop-optional-compat \
  uv run --locked --all-extras python scripts/check_optional_runtimes.py
```

El chequeo elimina configuración heredada `AIRFLOW_*`/`MLFLOW_*` y crea estado
temporal: metadatos SQLite de Airflow 3, configuración LocalExecutor y autenticación,
experimento/run/métrica/artefacto MLflow, ruta FastAPI sintética con TestClient y
round trips Fernet/Pillow. Esto comprueba APIs de dependencias; no demuestra una
API de producto, un DAG de negocio ni un modelo de priorización. El resumen queda
en `reports/optional/`; los metadatos y artefactos temporales se eliminan al salir.

## Verificar PostgreSQL, Compose y recuperación

Con Docker disponible, prepara un entorno del lock anterior y descarga las imágenes
exactas antes de ejecutar las sondas. La revisión anterior es una fixture de
compatibilidad aislada; sus hallazgos conocidos no quedan aceptados ni se usa como
runtime de trabajo:

```bash
mkdir -p /tmp/secop-runtime-baseline
git show 1ed08ebce37a421bd38a94d6450192a05a2103f9:pyproject.toml > /tmp/secop-runtime-baseline/pyproject.toml
git show 1ed08ebce37a421bd38a94d6450192a05a2103f9:uv.lock > /tmp/secop-runtime-baseline/uv.lock
UV_PROJECT_ENVIRONMENT=/tmp/secop-runtime-baseline-env \
  uv sync --directory /tmp/secop-runtime-baseline --locked --all-extras --no-install-project
docker pull postgres:16.4-alpine
docker pull apache/airflow:2.11.2-python3.12
docker pull apache/airflow:3.3.2-python3.12
UV_PROJECT_ENVIRONMENT=/tmp/secop-optional-compat \
  uv run --locked --all-extras python scripts/check_airflow_migration.py
UV_PROJECT_ENVIRONMENT=/tmp/secop-optional-compat \
  uv run --locked --all-extras python scripts/check_local_airflow_profile.py \
  --summary reports/optional/compose-profile.json \
  --baseline-python /tmp/secop-runtime-baseline-env/bin/python
```

La primera sonda crea exclusivamente contenedores/red propios, sin puertos de host:
metadatos sintéticos en Airflow 2.11.2, backup PostgreSQL, migración de una copia a
3.3.2, conservación de variable/conexión/pool y restauración del backup comprobada
con Airflow 2. Comprueba autenticación, denegación anónima y una tarea sintética por
scheduler/API de ejecución. No consulta bases ni configuración de trabajo.

La segunda ejecuta el Compose real con proyecto y `.env` temporales, puertos
loopback libres y volúmenes propios. Comprueba UID configurado, archivos secretos,
salud de los componentes, acceso autenticado/denegado y persistencia de credenciales
tras reiniciar. También guarda y recupera un experimento, una métrica y un artefacto
MLflow usando PostgreSQL y el SQLAlchemy del lock. Comprueba además metadatos
y artefactos sembrados con el lock anterior, su lectura con SQLAlchemy 2 y la
restauración de un backup en otra base leída por el entorno anterior. Esa evidencia
usa el backend PostgreSQL directo del cliente; no demuestra un servidor HTTP ni
que la imagen MLflow instale el mismo SQLAlchemy que el lock del proyecto. Ambas
sondas verifican la limpieza de sus recursos; solo publican resúmenes saneados, nunca contraseñas,
JWT, DSN o backups. Una falla de cleanup impide declarar éxito.

Estas pruebas no cubren todo estado histórico posible ni el pipeline orquestado de
negocio. Después ejecuta `make ci` con Java 17 y PostgreSQL dedicado según la
[guía de integración](continuous-integration.md): debe conservar precisión temporal,
inventario, dinero decimal, identidad, población y restauración A → B → A. Los
smokes no sustituyen esa integración ni los escaneos reales.

## Actualizar servicios existentes

Detén escritores y conserva backups restaurables de PostgreSQL y los artefactos
MLflow. Ensaya la migración sobre una copia aislada con las versiones anterior y
nueva, verifica datos retenidos y prueba restaurar el backup antes de adoptar el
cambio en tu servicio. Las sondas automatizadas no migran la base de trabajo.

Volver a una etiqueta o lock anterior no revierte esquemas: el rollback requiere
restaurar el estado correspondiente. El Compose conserva contraseñas y JWT en un
volumen local; sigue la [guía operativa](operations.md) para acceso y recuperación.
No copies credenciales de fixture ni archivos de prueba a servidores compartidos.

Registra versiones, hashes, plataforma, comandos y límites. Ejecuta `make security`
sobre la revisión final y conserva cualquier hallazgo como bloqueo. La revisión de
código, las pruebas funcionales y una auditoría sin hallazgos son evidencias
complementarias; ninguna garantiza ausencia universal de vulnerabilidades.
