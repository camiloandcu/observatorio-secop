# Primeros pasos

## Requisitos

- Windows con WSL2 y una distribución Ubuntu.
- Docker Desktop con **Settings → Resources → WSL Integration** habilitado para Ubuntu.
- Git y GNU Make dentro de WSL2.
- `uv` 0.12.x como herramienta obligatoria de desarrollo.

Comprueba el entorno desde WSL2:

```bash
docker version
docker compose version
git --version
uv --version
```

Si aún no tienes `uv`, instálalo con el método oficial para Linux y abre de nuevo la terminal:

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

El proyecto declara la versión aceptada de `uv` en `pyproject.toml`. `uv` administra Python 3.12 y el entorno virtual, por lo que no necesitas instalar el intérprete globalmente ni activar `.venv`.

## Ubicación del checkout

Guarda el repositorio en el filesystem Linux, por ejemplo `~/projects/observatorio-secop`. Evita `/mnt/c/...`: el filesystem de Windows puede causar I/O lento, diferencias de permisos y problemas con finales de línea. Los scripts usan LF mediante `.gitattributes`.

## Inicio rápido

Ejecuta todos los comandos desde la raíz del repositorio en WSL2:

```bash
make install
cp .env.example .env
make preflight
make compose-config
make up
make health
make quality
make down
```

`make up` inicia PostgreSQL, espera su health check y confirma que existen las bases `secop`, `airflow` y `mlflow`. Repetir el comando conserva el volumen y no duplica las bases.

`.env.example` contiene valores exclusivos para desarrollo local. No reutilices esas contraseñas fuera de este checkout ni agregues `.env` a Git.

## Servicios optativos

Airflow y MLflow no arrancan con el servicio base para ahorrar memoria y tiempo en Docker Desktop:

```bash
make up-airflow
make up-mlflow
make ps
```

- Airflow queda disponible en `http://localhost:8080` y usa su propia base PostgreSQL. No contiene DAGs de negocio.
- MLflow queda disponible en `http://localhost:5000`, usa PostgreSQL como backend y escribe artefactos locales en `mlruns/`, que Git ignora.

Los puertos se pueden cambiar en `.env` si ya están ocupados. Continúa con la [guía de desarrollo](development.md) o consulta la [guía de operación](operations.md).
