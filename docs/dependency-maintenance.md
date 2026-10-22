# Actualización compatible de dependencias

Las dependencias Python se resuelven juntas para los seis extras y quedan fijadas en
`uv.lock`. La revisión compatible usa Airflow 2.11.2, MLflow 3.17.2 y FastAPI 0.160;
conserva PySpark 3.5, PyArrow 19, pytest 8 y setuptools 75. Las imágenes opcionales
predeterminadas de `.env.example` y del Dockerfile MLflow coinciden con esas versiones.

La actualización no constituye una remediación completa. Airflow 2 está fuera de
soporte y sus restricciones Flask/FAB impiden corregir varios hallazgos sin otra
migración. Otros paquetes retenidos también tienen avisos pendientes. El auditor
continúa fallando ante ellos; no se incorporaron excepciones ni exclusiones.
Consulta el [seguimiento de remediación](https://github.com/camiloandcu/observatorio-secop/issues/34).

## Revisar una actualización

Antes de modificar el lock, consulta advisories y compatibilidad de productores y
consumidores. Prefiere actualizar los paquetes afectados y las cohortes que deban
cambiar juntas; por ejemplo, OpenTelemetry API/SDK/exportadores comparten versiones.
No fuerces dependencias fuera de los requisitos declarados por sus padres.

```bash
uv lock --upgrade-package paquete_afectado
make security-dependencies
```

La auditoría usa la exportación real de todos los extras. Un cambio legítimo del
grafo puede retirar dependencias que upstream hizo opcionales, como FastMCP/diskcache
en MLflow, sin eliminar extras del proyecto. El reporte comprueba exactamente los
nombres/versiones auditados y declara exclusiones de plataforma. La cantidad de
hallazgos puede cambiar con el feed aun sin modificar el lock.

## Verificar el entorno opcional resuelto

Usa un entorno aislado para instalar todos los extras sin alterar el entorno de
calidad habitual:

```bash
UV_PROJECT_ENVIRONMENT=/tmp/secop-optional-compat \
  uv run --locked --all-extras python scripts/check_optional_runtimes.py
```

El chequeo ignora configuración heredada `AIRFLOW_*`/`MLFLOW_*` y crea estado temporal:
metadatos SQLite de Airflow con `SequentialExecutor`, un experimento/run/métrica y
artefacto MLflow, una ruta FastAPI sintética con TestClient, y round trips de
Fernet/Pillow. Es una prueba de interfaces de dependencias; no demuestra una API,
DAG de negocio o modelo de priorización implementados. Solo conserva un resumen
bajo `reports/optional/`; los metadatos y artefactos temporales se eliminan al salir.

La instalación conjunta, el arranque de las imágenes y el backend PostgreSQL son
fronteras distintas. Complementa este chequeo con imágenes desechables de las
versiones seleccionadas y bases PostgreSQL exclusivas: migración/check de Airflow,
salud del servidor MLflow, experimento/run/métrica y rechazo de un Host no permitido.
No desactives la protección Host/CORS para hacer pasar una prueba.

Después ejecuta `make ci` con Java 17 y PostgreSQL dedicado según la
[guía de integración](continuous-integration.md). Deben conservarse los resultados
exactos de Spark, inventario, carga, dbt y restauración A → B → A. Un smoke opcional
no sustituye esa integración ni un escaneo real.

## Actualizar servicios con estado existente

Antes de usar las nuevas imágenes sobre un servicio existente, detén sus escritores
y conserva backups restaurables de PostgreSQL y los artefactos MLflow. Ensaya la
migración en una copia aislada con la versión anterior y la nueva, verifica lectura
de los datos retenidos y prueba restaurar el backup antes de cambiar el servicio.

Las pruebas sobre una base nueva no demuestran migración de todos los estados
históricos posibles. Volver a una etiqueta anterior no revierte una migración de
metadatos: el rollback requiere restaurar el estado correspondiente. El chequeo
automatizado usa exclusivamente estado desechable y no migra la base de trabajo.
Actualiza los pins del `.env` operativo de forma explícita al realizar esa operación;
no copies configuraciones de pruebas ni credenciales de fixture a un servidor compartido.

Registra versiones, hash del lock, plataforma, comandos, resultados y límites. Un
escaneo bloqueado sigue siendo un bloqueo, aunque los tests funcionales pasen.
