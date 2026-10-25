# Operar el pipeline Airflow

El DAG manual `secop_pipeline` ejecuta validación del contrato existente → adquisición
acotada → Silver con Spark → carga verificada → candidata dbt/tests → activación →
resumen. Los comandos de cada productor conservan sus reglas; Airflow no regenera
contratos ni fixtures y no cambia fórmulas. Los health checks y la importación del
DAG no demuestran que esas etapas hayan ejecutado.

## Preparar una demo aislada

Trabaja desde el checkout Linux de WSL2, con Docker Desktop integrado y `uv` según
[primeros pasos](getting-started.md). Conserva el entorno de trabajo existente.
Crea un archivo privado a partir de `.env.example` y edítalo localmente:

```bash
cp .env.example .env.pipeline-demo
chmod 600 .env.pipeline-demo
```

En ese archivo fija `COMPOSE_PROJECT_NAME=secop-pipeline-demo`,
`POSTGRES_DB=secop_pipeline_demo`, `POSTGRES_PORT=55432`, `AIRFLOW_PORT=18080` y
`SECOP_RUNTIME_MODE=demo`. Elige puertos libres y una contraseña local propia;
ninguna credencial debe pasarse al DAG, imprimirse o incluirse en evidencia.
El nombre del proyecto separa los volúmenes de otros entornos. Esta guía usa ese
archivo en todos los comandos; no omitas la selección al operar otro proyecto.

```bash
ENV_FILE=.env.pipeline-demo make preflight
ENV_FILE=.env.pipeline-demo make compose-config
ENV_FILE=.env.pipeline-demo make up-airflow
```

La construcción inicial puede descargar imagen y dependencias. Después, las
tareas usan la instalación fijada; el replay sintético no consulta Socrata ni AWS.
La distribución de recursos, permisos y secretos está en
[operaciones](operations.md#runtime-del-pipeline-airflow).

Define un helper en esta terminal para conservar la selección del entorno:

```bash
pipeline_compose() {
  docker compose --env-file .env.pipeline-demo --profile airflow "$@"
}
pipeline_compose ps
pipeline_compose exec -T airflow-api-server airflow pools set secop_writer 1 'Bounded analytical writer'
pipeline_compose exec -T airflow-api-server airflow dags list-import-errors
pipeline_compose exec -T airflow-api-server airflow dags unpause secop_pipeline
```

El pool tiene un solo escritor, igual que el límite de una ejecución activa del
DAG. Los locks de los productores y la generación de publicación siguen siendo
necesarios frente a comandos manuales. No ejecutes simultáneamente otro escritor
sobre las mismas raíces/base. Para acceder a la UI autenticada local, sigue
[la sección de acceso Airflow](operations.md); no copies su contraseña a capturas.

## Disparar y observar

En la UI de Airflow abre `secop_pipeline`, selecciona Trigger DAG y asigna un
Run ID único, por ejemplo `demo-a-001`. Usa exactamente este objeto de configuración
para la primera ejecución de la demo vacía:

```json
{
  "mode": "demo",
  "fixture_id": "demo-a",
  "signing_from": "2026-01-01",
  "signing_to": "2026-02-01",
  "baseline_version": null,
  "baseline_sha256": null,
  "total_rows": 100,
  "total_requests": 30,
  "grant_rows": 100,
  "grant_requests": 30,
  "page_size": 2,
  "build_seconds": 600,
  "storage_bytes": 134217728
}
```

Todos los campos son obligatorios. No se admiten conexiones, SQL, comandos,
endpoints ni rutas arbitrarias en el trigger. Las fechas tienen límite inferior
incluido y superior excluido. Los techos cuentan solicitudes, reintentos y probes
según el productor; terminar sin excepción no demuestra cobertura completa.
Consulta [población y recuperación](population-recovery.md) para su significado.

`demo-a` contiene dos contratos sintéticos y COP 300,00. `demo-b` contiene tres y
COP 400,00; `demo-rejected` provoca rechazo de calidad Silver. Estas fixtures no
son observaciones reales ni resultados de producción. Después de la primera
versión, una nueva ejecución exige el baseline compatible fijado: copia su versión
y hash verificados del recibo anterior en ambos campos, sin resolver `latest` en
cada etapa. Una configuración sin baseline no autoriza reemplazar una población.

La UI muestra `validate`, `acquire`, `silver`, `warehouse`, `candidate`, `activate`
y `outcome`. Sigue el estado y los códigos textuales, además del color. La candidata
incluye el build, tests y conciliaciones aisladas de dbt. Su fallo conserva la
publicación anterior aunque la carga raw ya haya avanzado.

El ID durable se deriva del DAG y Run ID; no es el ID de publicación. Calcúlalo
con el mismo paquete instalado y conserva el resultado no sensible:

```bash
pipeline_compose exec -T airflow-api-server python -c \
  'from observatorio_secop.orchestration.models import execution_id; print(execution_id("secop_pipeline", "demo-a-001"))'
```

Sustituye `EXECUTION_ID` por ese resultado:

```bash
pipeline_compose exec -T airflow-api-server python -m observatorio_secop.orchestration.runner \
  inspect --execution-id EXECUTION_ID
```

El resumen permitido identifica modo, campaña, publicación y estados, intentos,
filas, duraciones y versiones disponibles por etapa. Un campo todavía ausente no
es un valor cero ni una publicación inventada. Los recibos completos, páginas,
artefactos dbt y logs capturados permanecen privados en los volúmenes descritos en
operaciones; no publiques su contenido íntegro ni vuelques el entorno de procesos.

## Reintentar conservando el insumo

Los errores transitorios tienen como máximo dos reintentos automáticos, con
backoff inicial de 30 segundos y máximo de 120; cada tarea tiene límite de 30
minutos y el DagRun, de dos horas. Son límites configurados, no promesas de
rendimiento. Un error de contrato, alcance, cobertura, inventario, receta,
capacidad o generación obsoleta necesita diagnóstico, no otro insumo automático.

Tras inspeccionar, en la UI puedes limpiar únicamente la tarea fallida de ese
mismo DagRun y las tareas downstream que necesiten continuar. Conserva Run ID y
conf; evita seleccionar tareas upstream exitosas innecesariamente. La recuperación
verifica la misma campaña/grant, hashes, snapshot y publicación; un resultado ya
comprometido se inspecciona y reutiliza. Nunca se busca la carpeta más reciente.

Con el DAG pausado y sin otra tarea activa, el CLI permite reintentar una etapa
fijada, por ejemplo adquisición:

```bash
pipeline_compose exec -T airflow-api-server python -m observatorio_secop.orchestration.runner \
  stage --execution-id EXECUTION_ID --stage acquire
```

Los nombres de etapa son los de la UI. El CLI actualiza el recibo durable, no el
estado de la tarea en Airflow: para continuar por scheduler, limpia esa tarea y
sus downstream pendientes en el mismo DagRun; se reutilizará el resultado verificado.
También puedes continuar por CLI las etapas pendientes en orden, con el DAG pausado. No ejecutes el CLI y la tarea Airflow a la
vez. Repetir `validate` usa el binding existente y no autoriza cambiar la receta.
Un contrato o artefacto cambiado requiere resolver el conflicto; no borres recibos
para hacer pasar el reintento.

Si adquisición informa `PIPELINE_COVERAGE_INCOMPLETE`, inspecciona primero el motivo
y la campaña. Un grant agotado puede ampliarse explícitamente dentro de los techos
originales; el scheduler nunca lo repone por sí solo:

```bash
pipeline_compose exec -T airflow-api-server python -m observatorio_secop.orchestration.runner \
  extend-grant --execution-id EXECUTION_ID --grant-id operator-2 --rows 20 --requests 10
pipeline_compose exec -T airflow-api-server python -m observatorio_secop.orchestration.runner \
  stage --execution-id EXECUTION_ID --stage acquire
```

El nuevo ID de grant no se reutiliza ni reactiva uno anterior. Si se agotó el techo
total, hace falta un plan/ejecución nuevo explícito; la extensión no reinicia
contadores, reservas inciertas ni checkpoint incremental. Un truncamiento terminal
o rechazo de evidencia no se corrige aumentando grants a ciegas.

## Diagnosticar publicación y aislamiento

Pausa nuevos escritores antes de operar manualmente una candidata. Su ID figura
en el resumen. La inspección detallada puede incluir metadatos privados: guárdala
localmente con permisos restrictivos y revisa solo campos necesarios.

```bash
mkdir -p reports/pipeline-private
umask 077
pipeline_compose exec -T airflow-api-server secop-publication inspect \
  --publication-id PUBLICATION_ID > reports/pipeline-private/publication.json
```

| Estado/código | Acción del operador |
|---|---|
| Candidata `building`/`failed` compatible | Inspeccionar y reintentar la misma etapa `candidate`; usa la receta y budgets fijados |
| Candidata `sealed` | Reutilizar el sellado y continuar activación con las expectativas originales |
| Publicación ya activa idéntica | Confirmar no-op; no crear otro evento para suplir un acknowledgement perdido |
| `PUBLICATION_STALE` / `PIPELINE_STALE_PUBLICATION` | Inspeccionar escritor y generación ganadores; no sustituir automáticamente el expected parent |
| `PIPELINE_ACTIVATION_SUPERSEDED` | La activación original fue reemplazada; decidir recuperación explícita, sin restauración automática |
| `PUBLICATION_CAPACITY` | Revisar retención/cleanup permitido; no eliminar versión activa o rollback ni acortar plazos |
| `PIPELINE_INPUT_CHANGED` / `PIPELINE_ARTIFACT_CHANGED` | Conservar evidencia y resolver receta/hash; no reescribir identidad histórica |
| `PIPELINE_RUNTIME_BINDING_CONFLICT` / `PIPELINE_RUNTIME_MODE_MISMATCH` | Comprobar proyecto, modo, raíces y base; no editar marcadores para eludir la separación |
| `PIPELINE_UNBOUND_AUTHORITATIVE_STATE` | Usar entorno aislado o revisar adopción viva explícita con baseline exacto, nunca inferir autoridad |

Para reintentar una candidata de esa ejecución sin cambiar receta, identidad ni
budgets, con el DAG pausado:

```bash
pipeline_compose exec -T airflow-api-server python -m observatorio_secop.orchestration.runner \
  stage --execution-id EXECUTION_ID --stage candidate
```

El adapter inspecciona su estado antes de llamar al productor: no reconstruye una
candidata sellada. Si decides descartar una candidata privada inspeccionada, con
sus procesos detenidos:

```bash
pipeline_compose exec -T airflow-api-server secop-publication discard \
  --publication-id PUBLICATION_ID > reports/pipeline-private/discard.json
```

El descarte no es un reintento ni restaura capas raw. Para un retry manual del
productor fuera del DAG consulta [publicación analítica](analytical-publication.md);
no alternes ese camino con el DAG activo. La limpieza siempre empieza con
`secop-publication cleanup-preview` y selecciona IDs explícitos elegibles.

El modo queda vinculado durablemente a raíces y base. Cambiar `SECOP_RUNTIME_MODE`
o usar el mismo warehouse con volúmenes distintos no convierte demo en vivo. Un
entorno vivo requiere proyecto/base/volúmenes separados, `mode=live`,
`fixture_id=null` y alcance/budgets explícitos. La adopción de estado vivo existente
verifica baseline exacto Silver/raw y scope compatible; una base sin binding no se
considera segura por su nombre. No mezcles datos sintéticos y autoridad viva.

## Restaurar A después de B

No hay transacción distribuida entre Silver, raw warehouse y publicación. Detén los
escritores, inspecciona cada capa y conserva separadamente sus identidades actuales;
pueden diferir tras un fallo. Solo restaura versiones A previamente registradas y
compatibles. Con IDs/hashes verificados, los comandos son:

```bash
pipeline_compose exec -T airflow-api-server secop-silver restore \
  --version VERSION_A --manifest-sha256 HASH_A \
  --expected-current-version CURRENT_SILVER --expected-current-manifest-sha256 CURRENT_SILVER_HASH
pipeline_compose exec -T airflow-api-server secop-warehouse restore \
  --version VERSION_A --manifest-sha256 HASH_A \
  --expected-current-version CURRENT_RAW --expected-current-hash CURRENT_RAW_HASH
pipeline_compose exec -T airflow-api-server secop-publication restore \
  --publication-id PUBLICATION_A --expected-publication-id CURRENT_PUBLICATION \
  --expected-generation CURRENT_GENERATION > reports/pipeline-private/restore.json
```

Estos valores son placeholders de identidades inspeccionadas, no comandos de
selección automática. Si falla un expected-state, vuelve a inspeccionar; no lo
refresques silenciosamente. Restaurar solo la publicación A retenida conserva su
contenido inmutable y crea el evento correspondiente, pero no restaura Silver/raw.
Una A eliminada informa no disponible y requiere reconstrucción validada con nueva
identidad, no reutilizar la antigua.

Verifica después versión/hash de Silver y raw, publicación activa/generación,
conteos, llaves, valor decimal COP y evidencia de calidad contra el registro de A.
Repetir la versión ya activa debe ser no-op. Los contratos y verificaciones de
capa están en [recuperación de población](population-recovery.md),
[modelo estrella](star-schema.md) y [publicación](analytical-publication.md).
Conserva los resultados reales del ensayo; esta secuencia documentada no demuestra
por sí sola una restauración exitosa.

## Detener y extender

```bash
ENV_FILE=.env.pipeline-demo make down
```

La parada conserva volúmenes, recibos y publicación. `make clean` es eliminación
destructiva explícita; no es rollback ni reparación de permisos.

Una etapa futura de scoring consume el ID inmutable de la publicación activada y
su compatibilidad revisada; no vuelve a resolver `latest` ni duplica fórmulas del
pipeline. Scoring ausente/fallido deja analítica válida disponible y no inventa
scores. Un descriptor lector se resuelve al comenzar una lectura posterior a la
activación, respetando su vencimiento, no al iniciar el pipeline largo.
