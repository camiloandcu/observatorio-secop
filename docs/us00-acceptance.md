# Evidencia de aceptación de US-00

Fecha de ejecución: 2026-09-22. Entorno: Ubuntu sobre WSL2, Docker Desktop 4.89.0, Docker Engine 29.7.2 y Docker Compose 5.5.0.

## Resultado

- `docker compose config` resolvió la configuración y enumeró los perfiles `airflow` y `mlflow`.
- PostgreSQL 16.4 alcanzó estado `healthy`; se verificaron las bases `secop`, `airflow` y `mlflow`.
- Dos inicios consecutivos conservaron una fila de prueba y no duplicaron bases.
- Airflow 2.10.5 inició webserver y scheduler saludables; `/health` confirmó metadatabase y scheduler.
- MLflow 3.4.0 inició saludable con backend PostgreSQL y usuario no root.
- El flujo de calidad corregido se ejecutó mediante `uv` 0.12.0 y Python 3.12.13: formato y lint aprobados, 22 pruebas aprobadas y cero rutas prohibidas rastreadas.
- El workflow CI se parseó como YAML y sus pruebas confirmaron instalación con `uv`, resolución bloqueada de Python 3.12, calidad mediante Make y ausencia de secretos o servicios pesados.

## Fallos controlados

- Una variable `POSTGRES_PASSWORD` vacía hizo fallar la interpolación antes de crear servicios y nombró la variable.
- Un proceso ocupando el puerto alternativo `55432` impidió publicar PostgreSQL; el entorno principal y su volumen no fueron alterados.
- Un `DOCKER_HOST` inexistente hizo fallar `preflight` con instrucciones para iniciar Docker Desktop y habilitar WSL Integration; el reintento posterior fue exitoso.
- Una ruta simulada bajo `/mnt/c` produjo la recomendación del filesystem Linux sin mover archivos.
- La limpieza sin `CONFIRM_CLEAN=yes` fue rechazada.
- Las pruebas controladas demostraron que formato, lint, smoke tests y artefactos locales rastreados producen códigos de error observables.

## Reproducibilidad y limitaciones observadas

La aceptación inicial usó un contenedor limpio con Python 3.12.11 porque WSL no tenía ese intérprete instalado. El flujo fue corregido después para requerir `uv`, que administra Python 3.12, `.venv` y las dependencias bloqueadas sin exigir una instalación global del intérprete.

El helper de credenciales de Docker Desktop devolvió un error al descargar anónimamente desde GHCR. La comprobación se completó usando un `DOCKER_CONFIG` temporal vacío, sin modificar credenciales del usuario. Una vez descargada la imagen oficial, el build y el health check de MLflow fueron exitosos.

En la primera reconstrucción limpia, el webserver de Airflow agotó su timeout interno al intentar iniciar cuatro workers con recursos locales limitados. El perfil se corrigió a un worker, un proceso de parsing y un timeout de arranque de 300 segundos; la validación final se repitió con esa configuración.
