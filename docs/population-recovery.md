# Cobertura y recuperación acotada

La población analítica comprende contratos de entidades registradas en Antioquia con fecha de firma en un intervalo explícito `[desde, hasta)`. Se excluyen las firmas ausentes. La ubicación de la entidad no demuestra dónde se ejecuta el contrato.

La recuperación consulta el intervalo de firma sin depender de la fecha de actualización. Si existe una versión base, también consulta sus identificadores sin restricciones de firma o actualización. Una respuesta ausente queda como no confirmada; no demuestra una eliminación. Una corrección verificada que saca un contrato base del alcance bloquea la activación y conserva la versión anterior.

## Adquirir e inspeccionar

Ejemplo acotado; ajusta el intervalo y los presupuestos antes de consultar la fuente:

```bash
uv run secop-ingest reconcile \
  --signing-from 2026-01-01 --signing-to 2026-02-01 \
  --total-rows 1000 --total-requests 30 \
  --grant-rows 200 --grant-requests 8 --page-size 50
uv run secop-ingest inspect-campaign CAMPAIGN_ID
```

Para recuperar sobre una base añade `--baseline data/silver/secop_contracts/versions/VERSION/manifest.json`. El manifiesto se verifica y fija por hash. Para reanudar, repite la misma configuración con `--resume CAMPAIGN_ID` y una concesión explícita; los límites totales no se recargan. Usa las rutas configuradas si difieren de las del ejemplo.

Cada solicitud física reserva previamente un intento y su capacidad máxima de filas. Esto incluye metadatos, reintentos y consultas terminales. Una interrupción incierta conserva el cargo. Una página de exploración llena requiere una consulta posterior para demostrar agotamiento; el último lote fijo de identificadores no requiere esa consulta adicional. Si falta presupuesto, inspecciona el motivo y reanuda dentro del techo original. El checkpoint incremental no cambia.

## Construir y verificar

```bash
uv run secop-silver run-population \
  --signing-from 2026-01-01 --signing-to 2026-02-01 \
  --campaign-run-id RUN_ID --mode preview
uv run secop-silver inspect --version VERSION
uv run secop-warehouse verify --version VERSION --manifest-sha256 HASH
```

Repite `--campaign-run-id` para incorporar adquisiciones. Si hay base, pasa también `--baseline-version VERSION_BASE --baseline-manifest-sha256 HASH_BASE`. Primero revisa la evidencia. El modo `preview` nunca selecciona una versión ni carga el warehouse, incluso al reutilizarla o intentar restaurarla. Para una candidata autoritativa usa `--mode authoritative_candidate`; la activación exige cobertura completa, alcance compatible y conservación de las llaves base.

`complete` significa agotamiento verificado del plan acotado observado. No garantiza una instantánea de la fuente, vigencia universal ni ausencia de cambios concurrentes. `partial` y `sample` conservan sus limitaciones y no activan la población autoritativa. Los contratos sin firma siguen excluidos aunque termine el plan.

El manifiesto Silver 3 mantiene el perfil contractual 2 e inventaría cuatro evidencias: cobertura, conciliación, resumen de condición y enlaces de rechazo. Sus hashes, esquemas, conteos y referencias se verifican antes de abrir la conexión de carga. El resumen tiene una fila por contrato; los enlaces conservan cardinalidad 1:N y no deben unirse directamente al hecho para sumar valores.

Una observación inválida adquirida estrictamente después del ganador válido se señala sin sustituirlo. La comparación usa tiempo de adquisición UTC, no fecha de actualización de la fuente. Los rechazos permanecen como evidencia separada; esta condición no define una política de entrenamiento ni prueba irregularidades.

## Cargar, adoptar y restaurar

```bash
uv run secop-warehouse load --version VERSION --manifest-sha256 HASH
```

La transacción conserva datos, cobertura, condiciones, registro e historial juntos. Una candidata obsoleta, incompleta, con otro alcance o que pierde miembros se rechaza. Repetir la identidad ya activa es un no-op solamente si la evidencia persistida coincide.

Las versiones históricas de manifiesto 1/2 siguen siendo verificables. Las construcciones nuevas sin alcance son vistas previas. Una versión histórica activa puede adoptarse verificando su contenido y cierre Bronze, sin reescribir el manifiesto ni declarar cobertura completa:

```bash
uv run secop-silver adopt --version LEGACY --manifest-sha256 HASH \
  --signing-from 2026-01-01 --signing-to 2026-02-01
uv run secop-warehouse adopt --version LEGACY --manifest-sha256 HASH \
  --bronze-root data/bronze/secop_contracts
```

La adopción fija un alcance persistente. Las versiones heredadas conservan cobertura desconocida. No se puede adoptar una población fuera del intervalo o sin pruebas suficientes.

Silver y PostgreSQL mantienen selecciones independientes; no hay transacción entre ambos. Inspecciona las dos identidades tras una interrupción y reintenta la carga fijada. Para restaurar una versión previamente registrada y compatible, fija también la identidad que esperas encontrar activa:

```bash
uv run secop-silver restore --version TARGET --manifest-sha256 TARGET_HASH \
  --expected-current-version CURRENT --expected-current-manifest-sha256 CURRENT_HASH
uv run secop-warehouse restore --version TARGET --manifest-sha256 TARGET_HASH \
  --expected-current-version CURRENT --expected-current-hash CURRENT_HASH
```

Los valores `CURRENT` pueden diferir entre almacenes. Si otro escritor cambió la selección, inspecciona el estado antes de repetir; no sustituyas automáticamente la identidad esperada. Una restauración no modifica el alcance persistente ni convierte una ausencia de fuente en eliminación.

Los consumidores dbt disponen de `staging.stg_population_coverage`, `staging.stg_contract_observation_condition` y `staging.stg_contract_observation_link`. Los dos últimos mantienen separados el resumen 1:1 y la evidencia 1:N. Consulta [el modelo estrella](star-schema.md) para configurar PostgreSQL y dbt.
