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

## Health checks y observabilidad

- PostgreSQL usa `pg_isready` y una consulta confirma las bases de aplicación, Airflow y MLflow.
- El webserver de Airflow consulta `/health`.
- El scheduler de Airflow usa `airflow jobs check`.
- MLflow consulta `/health` con la biblioteca estándar de Python.
- `make ps` muestra estado y health; `make logs SERVICE=<nombre>` limita la salida a las últimas 200 líneas.

Un servicio en estado `unhealthy`, `exited` o `dead` hace fallar la espera inmediatamente. Los demás estados esperan como máximo `SERVICE_HEALTH_TIMEOUT` segundos.

## Parada y limpieza

`make down` detiene los servicios y conserva los volúmenes. La limpieza destructiva es un comando separado:

```bash
make clean
```

**Advertencia:** `make clean` elimina contenedores, el volumen PostgreSQL del proyecto y `mlruns/`. No elimina archivos fuente, `.env`, datos fuera del proyecto ni imágenes Docker.

## Diagnóstico

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
make logs SERVICE=airflow-webserver
make logs SERVICE=airflow-scheduler
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
