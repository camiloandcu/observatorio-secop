# Operación local

## Contrato operativo

La ruta soportada es Ubuntu en WSL2, con el checkout dentro del filesystem Linux y Docker Desktop como motor. PostgreSQL es el servicio mínimo. Airflow y MLflow son perfiles optativos y no ejecutan lógica de negocio en US-00.

Los puertos solo se publican en `127.0.0.1`. La configuración falla antes del arranque si falta una variable obligatoria. `down` conserva el estado y `clean` elimina únicamente el estado local descrito en su advertencia.

## Comandos

```bash
make compose-config  # resuelve y valida Compose antes de crear contenedores
make up              # inicia PostgreSQL y espera salud
make health          # confirma PostgreSQL y las tres bases
make ps              # muestra servicios base y perfiles
make logs SERVICE=postgres
make down            # detiene servicios y conserva volúmenes
```

Para seleccionar otro archivo de configuración local usa `ENV_FILE=/ruta/archivo`. El script no ejecuta `source` sobre ese archivo y nunca imprime sus valores.

Esa selección es para los servicios locales. Silver y warehouse leen `SILVER_ROOT` exportado o su propio YAML; ninguno importa `.env` automáticamente. Si no encuentran `current.json`, revisa la raíz efectiva en la misma shell; no copies el puntero ni adivines otra carpeta. La [guía de configuración y compatibilidad](star-schema.md#configuración-local) explica la precedencia y el consumo de bundles existentes.

Para recuperar un snapshot anterior sigue la [secuencia A → B → A](star-schema.md#volver-a-una-versión-silver-anterior): seleccionar el puntero Silver, verificar el bundle y cargar PostgreSQL son pasos separados. El no-op exige que versión y hash ya estén activos. Antes de actualizar un warehouse existente, revisa la [migración de auditoría](star-schema.md#migrar-una-auditoría-existente), detén escritores anteriores y conserva respaldos.

La [publicación analítica](analytical-publication.md) tiene activación y rollback
propios: construye sobre copias verificadas, valida, sella y cambia las vistas
juntas. BI/API fijan un descriptor durante toda la operación; la guía explica
roles, límites, expiración, reintento y limpieza segura de versiones retenidas.

La [migración de identidad de plataforma](platform-identity.md#compatibilidad-y-migración)
agrega columnas dentro de la transacción de carga y requiere un primer rebuild dbt con
`--full-refresh`. Conserva bundles anteriores y verifica contratos, llaves y valores antes
de reanudar consumidores. `refresh-identity` usa IDs de un baseline fijado y progreso
separado: no modifica el checkpoint incremental ni permite publicar solo el subset refrescado.

## Health checks y observabilidad

- PostgreSQL usa `pg_isready` y una consulta confirma las bases de aplicación, Airflow y MLflow.
- El API server de Airflow consulta `/api/v2/monitor/health`. Esta sonda de disponibilidad no sustituye la prueba de autenticación.
- El scheduler y el DAG processor separado usan `airflow jobs check`.
- MLflow consulta `/health` con la biblioteca estándar de Python.
- `make ps` muestra estado y health; `make logs SERVICE=<nombre>` limita la salida a las últimas 200 líneas.

Un servicio en estado `unhealthy`, `exited` o `dead` hace fallar la espera inmediatamente. Los demás estados esperan como máximo `SERVICE_HEALTH_TIMEOUT` segundos.

## Parada y limpieza

`make down` detiene los servicios y conserva los volúmenes. La limpieza destructiva es un comando separado:

```bash
make clean
```

**Advertencia:** `make clean` elimina contenedores, los volúmenes PostgreSQL y Airflow del proyecto y `mlruns/`. No elimina archivos fuente, `.env`, datos fuera del proyecto ni imágenes Docker.

## Diagnóstico

### Silver rechaza archivos o inventario

Ejecuta `uv run --locked --extra warehouse secop-warehouse verify` con la misma raíz y configuración que usarás para cargar. Revisa la ruta relativa indicada en el error y las [reglas de inventario y auxiliares](star-schema.md#archivos-verificados-y-auxiliares). Un Parquet extra se rechaza incluso si los conteos coinciden; las rutas ocultas y particiones no están exentas.

Conserva la evidencia; no edites hashes, manifiestos ni archivos para forzar aceptación. Selecciona un bundle íntegro retenido y sigue la [recuperación A → B → A](star-schema.md#volver-a-una-versión-silver-anterior), o reconstruye desde Bronze en otra raíz. El rechazo ocurre antes de conectar a PostgreSQL y conserva el snapshot y su auditoría. Cambiar el puntero Silver, verificar y cargar son operaciones separadas.

### Docker no responde

`make preflight` termina con error antes de crear servicios. Inicia Docker Desktop y habilita la distribución en **Settings → Resources → WSL Integration**; luego repite `make up`.

### Falta una variable

`make validate-env` indica únicamente el nombre ausente. Restaura la línea desde `.env.example` y repite `make compose-config`.

### Puerto ocupado

Docker informa el puerto que no puede publicar y no limpia automáticamente. Cambia `POSTGRES_PORT`, `AIRFLOW_PORT` o `MLFLOW_PORT` en `.env` y reintenta.

### Un servicio no alcanza salud

```bash
make ps
make logs SERVICE=postgres
make logs SERVICE=airflow-api-server
make logs SERVICE=airflow-scheduler
make logs SERVICE=airflow-dag-processor
make logs SERVICE=mlflow
```

### Checkout bajo `/mnt/c`

Ejecuta `make check-path`. Si detecta `/mnt/*`, mueve el checkout a `~/projects` desde WSL2. El diagnóstico no mueve ni modifica archivos automáticamente.

### Errores de permisos en cachés Python

No ejecutes `uv`, Ruff, pytest ni los destinos de Make con `sudo`. Si una ejecución anterior creó cachés como `root`, elimínalas una sola vez:

```bash
sudo rm -rf -- .ruff_cache .pytest_cache
```

Después ejecuta `make quality` como usuario normal.

## Recuperación

1. Ejecuta `make ps` y revisa el servicio afectado con `make logs SERVICE=...`.
2. Corrige Docker Desktop, la variable o el puerto reportado.
3. Repite el mismo comando de inicio; la inicialización de bases es idempotente y conserva el volumen.
4. Usa `make clean` solo cuando necesites reconstruir PostgreSQL y los artefactos de MLflow desde cero.

## Airflow 3: acceso y estado local

El perfil usa Airflow 3.3.2 con LocalExecutor, API server, scheduler y DAG processor
separado. El inicio crea un secreto JWT compartido y una contraseña aleatoria de
`admin` en el volumen `airflow_local_state`, con archivos de modo `600`. No se
sobrescriben al reiniciar. SimpleAuthManager exige autenticación; el modo anónimo
`all_admins` está desactivado. Este perfil solo admite desarrollo local por loopback.

Para abrir la sesión, consulta la contraseña únicamente en una terminal privada:

```bash
docker compose --env-file .env exec -T airflow-api-server \
  cat /opt/airflow/local-state/passwords.json
```

Usa el archivo indicado por `ENV_FILE` si seleccionaste otra configuración. No
copies la salida en logs, capturas, commits ni evidencia. Los comandos habituales
no imprimen estos secretos. `make down` conserva credenciales y logs; `make clean`
elimina también ese volumen y genera credenciales nuevas en el próximo arranque.

## Actualizar o recuperar metadatos Airflow

No ejecutes `make up-airflow` sobre una base Airflow 2 existente antes de preparar
la migración: `airflow-init` ejecuta `airflow db migrate`. Actualiza el pin
`AIRFLOW_IMAGE` de tu configuración local únicamente después del ensayo; copiar
`.env.example` encima de `.env` perdería tus valores locales.

1. Detén todos los escritores Airflow y registra la versión de imagen y del lock
   anterior. Conserva DAGs, configuración, logs y secretos locales fuera de Git.
2. Obtén un respaldo PostgreSQL consistente de la base de metadatos Airflow con
   `pg_dump --format=custom`. Conserva el respaldo con permisos restrictivos; puede
   contener credenciales de conexiones. No respaldes otra base por nombre supuesto.
3. Restaura con `pg_restore` en una base y proyecto Compose desechables separados,
   con puertos distintos. Ejecuta primero la migración allí con la nueva imagen.
4. Comprueba variables, conexiones, pools y demás registros que uses, arranque de
   los tres procesos, acceso autenticado y rechazo sin token. Ejecuta una tarea
   sintética por scheduler y Execution API; un health check no prueba su ejecución.
5. Ensaya la recuperación restaurando el respaldo previo en otra base desechable
   y usando la imagen y lock anteriores. Verifica sus registros antes de adoptar
   la actualización. Cambiar solo la imagen no revierte el esquema de metadatos.
6. Solo después de revisar ese resultado adopta la copia migrada y reanuda
   escritores. Si falla, mantén los escritores detenidos y recupera el respaldo
   previo con la versión anterior; conserva la copia fallida para diagnóstico.

La guía de [dependencias](dependency-maintenance.md) registra el alcance de los
ensayos automatizados. Ningún ensayo desechable actualiza la base de trabajo ni
prueba DAGs de negocio futuros. No ejecutes `make clean` como mecanismo de rollback.
