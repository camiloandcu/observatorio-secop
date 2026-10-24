# Publicación analítica validada

Los consumidores leen una publicación completa y validada, identificada por un ID
inmutable. Cada una reúne una versión Silver, manifiesto y población; incluye la
estrella, los cinco marts base y evidencia de calidad. La construcción dbt de
desarrollo sigue disponible, pero sus tablas mutables no son una publicación.

## Preparar y construir

Instala los extras fijados según [desarrollo](development.md). Configura el acceso
operador con `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER` y
`POSTGRES_PASSWORD`, fuera del repositorio. Reutiliza `config/warehouse.yaml` y la
misma raíz Silver del cargador. Los comandos requieren el extra `warehouse`.

El bootstrap requiere una base dedicada y un operador que pueda administrar sus
roles y objetos. Crea `secop_publication_owner` y `secop_publication_builder`
sin login y los lectores `secop_bi_reader` / `secop_api_reader` sin privilegios
administrativos. El operador conserva su responsabilidad de administración;
el aislamiento protege frente a constructores y consumidores, no frente al DBA.
El bootstrap también revoca CREATE público en el schema `public`: revisa ese
alcance antes de aplicarlo a una instalación existente. No modifica contraseñas
ni habilita autenticación por su cuenta.

```bash
uv run --locked --extra warehouse secop-publication bootstrap
uv run --locked --extra warehouse secop-publication inspect
```

Carga primero el bundle Silver verificado mediante el procedimiento existente.
Anota su versión y hash exactos. La publicación comprueba que coincidan con la
versión activa del warehouse y copia sus seis entradas en una transacción; un
avance posterior del warehouse no cambia esa copia. No carga un hijo arbitrario
en un workspace vacío ni evita los controles de población.

Construye con presupuestos explícitos, positivos y finitos:

```bash
uv run --locked --extra warehouse secop-publication build \
  --version "$SILVER_VERSION" --manifest-sha256 "$SILVER_MANIFEST_SHA256" \
  --timeout-seconds "$PUBLICATION_BUILD_TIMEOUT_SECONDS" \
  --storage-budget-bytes "$PUBLICATION_STORAGE_BUDGET_BYTES"
```

Elige los presupuestos a partir del tamaño y tiempo medidos de tu población;
no copies límites de fixtures como capacidad productiva. Se comprueba el espacio
de relaciones y se registran bytes, duración y espera de captura/construcción.
El presupuesto de relaciones no garantiza espacio libre del filesystem ni mide
WAL, capas del contenedor o toda la memoria del proceso: verifica también la
capacidad libre del host antes de una corrida real. Artefactos y logs quedan en
`reports/publication`, con una carpeta por intento; no publiques esos archivos.

El comando ejecuta todo el grafo dbt en namespaces privados como constructor
restringido. Exige todos los modelos y tests requeridos, conciliación exacta,
catálogo/política revisados y evidencia ligada al contenido. Después transfiere
las once tablas a propiedad controlada y crea proyecciones seguras. Devuelve
estado `sealed`, ID y la identidad/generación activa esperada. Todavía no concede
lectura del candidato. Nunca reconstruyas un namespace sellado.

## Activar y restaurar

Usa exactamente la expectativa que devolvió la construcción:

```bash
uv run --locked --extra warehouse secop-publication activate \
  --publication-id "$PUBLICATION_ID" \
  --expected-publication-id "$EXPECTED_PUBLICATION_ID" \
  --expected-generation "$EXPECTED_GENERATION"
```

Para la primera activación omite `--expected-publication-id` y usa generación 0.
Las vistas estables `published`, el puntero y el evento cambian juntos. Una falla
revierte el conjunto completo; una expectativa obsoleta devuelve `PUBLICATION_STALE`.
Inspecciona y decide explícitamente si quieres reintentar con la nueva expectativa.
Se compara también la generación para detectar A → C → A durante una construcción.

Una restauración retenida no recarga el warehouse ni altera la fecha de
construcción de A. Reactiva exactamente sus datos, calidad y proyecciones y añade
un evento de restauración:

```bash
uv run --locked --extra warehouse secop-publication restore \
  --publication-id "$PREVIOUS_PUBLICATION_ID" \
  --expected-publication-id "$CURRENT_PUBLICATION_ID" \
  --expected-generation "$CURRENT_GENERATION"
```

Repetir la identidad ya activa, con contenido y permisos intactos, es no-op sin
nuevo evento. Tras perder una confirmación, inspecciona y reintenta el mismo ID;
no inventes otro para ocultar la incertidumbre. Una publicación eliminada se
reporta no disponible: reconstruir desde un bundle retenido exige nuevo ID y
validación. El rollback completo Silver/warehouse conserva su procedimiento
independiente en [recuperación](population-recovery.md).

## Consumo fijado entre consultas

Configura credenciales propias del rol lector con el administrador, usando
variables de entorno fuera del repositorio. Comprueba lectura permitida de las
vistas y denegación de fuentes, candidatos, registros privados y escrituras.
No uses las credenciales del operador para Power BI o API.

Cada publicación ofrece `publication_<id>` con once vistas y
`publication_metadata`: identidad, versión/hash/scope, fechas de construcción,
observación de adquisición y finalización del pipeline, cobertura y revisiones de
catálogo/política. La estrella conserva llaves seguras; las proyecciones excluyen
rutas Bronze/locales y enlaces de proceso no admitidos. Los miembros desconocidos,
motivos nulos, condiciones retenidas y denominadores mantienen su significado.
Consulta [Gold](gold-analytics.md) para granos, relaciones y filtros.

Resuelve **una vez al iniciar** una operación:

```bash
uv run --locked --extra warehouse secop-publication resolve > /tmp/secop-publication-descriptor.json
uv run --locked --extra warehouse secop-publication read \
  --descriptor /tmp/secop-publication-descriptor.json --relation mart_contract_detail
uv run --locked --extra warehouse secop-publication query \
  --descriptor /tmp/secop-publication-descriptor.json --metric overview
```

`query` reutiliza las macros SQL gobernadas sobre el detalle fijado; admite
`overview`, `entity_monthly`, `concentration`, `detail` y calidad contractual
`quality`. `--filters` recibe un archivo JSON de valores, nunca SQL ni nombres de
relación. La calidad global permanece en `mart_data_quality`; no atribuyas sus
rechazos a una selección contractual. Los helpers limitan cada lectura a 100
filas y usan orden determinístico; el consumidor API define su cursor, filtros y
orden de negocio. Las consultas tienen un timeout finito de 60 segundos por
defecto; la API Python permite configurar `statement_timeout_ms` positivo según
mediciones. Una consulta larga no retiene el bloqueo global de activación.

El descriptor dura una hora según reloj PostgreSQL y no se renueva durante la
misma operación. Solo se admite si hay una hora completa de disponibilidad.
Valida el mismo descriptor en consultas y páginas posteriores; nunca resuelvas
`latest` por página. Ante `PUBLICATION_EXPIRED`, `PUBLICATION_EXPIRING` o
`PUBLICATION_UNAVAILABLE`, termina la operación y reiníciala completa con una
nueva selección. `PUBLICATION_DENIED` indica que el operador debe revisar los
permisos; `PUBLICATION_QUERY_TIMEOUT` requiere diagnosticar la consulta/presupuesto.

El descriptor fija consistencia, no concede autorización. Su vencimiento no
revoca SQL directo sobre una vista todavía retenida. Las vistas estables
`published` sirven para descubrir la actual o ejecutar una consulta individual;
varias consultas independientes deben usar el namespace fijado.

El campo `code_revision` identifica los bytes ejecutados mediante SHA-256 del
código Python, lockfile, configuración del paquete y wrapper de construcción,
incluso cuando aún hay cambios sin commit; el grafo dbt tiene su digest separado.
Los cálculos gobernados rechazan `PUBLICATION_CONTRACT_UNAVAILABLE` si el grafo
instalado difiere del publicado. Usa la revisión retenida compatible o consulta
sus vistas materializadas; no reinterpretas la publicación antigua con macros
nuevas. El código/catálogo/política se fijan antes del primer intento: un cambio
requiere descartar el candidato privado y construir otro ID, también tras fallos.

Para Power BI Import, convierte los valores del descriptor en un **parámetro
literal compartido** antes de Refresh All, sin leer un archivo o resolver latest
independientemente en cada consulta. [La función M de conexión](examples/publication-import.pq)
valida ID, reloj y disponibilidad y sirve a todas las relaciones y metadatos.
Mantén el parámetro inmóvil durante el refresh; ante error, conserva el último
Import válido y reintenta todo el refresh con otro descriptor. La verificación
real de Desktop, relaciones, filtros y conservación del Import debe realizarse
al construir el reporte; una plantilla y pruebas SQL no demuestran ese recorrido.

Para API, transporta el mismo ID/plazo en su paginación y valida su integridad,
filtros y orden en el adaptador HTTP. Este módulo provee el límite SQL; no añade
rutas HTTP. Features/exportaciones/orquestación identifican la misma publicación
y deben revisar su duración si necesitan operaciones más largas.

## Retención, fallos y limpieza

Se retienen hasta tres publicaciones y un candidato privado. La activa y la
anterior inmediata siempre están protegidas. Cada desactivación y cada salida
del conjunto protegido extiende la retención mínima 24 horas: un lector recién
fijado a una antigua versión de rollback conserva su horizonte aun cuando esta
deje de ser la anterior. Si no hay cupo seguro, `PUBLICATION_CAPACITY` bloquea la
nueva construcción; nunca borres anticipadamente para forzarla.

Un candidato fallido/huérfano ocupa su único cupo. Inspecciona su ID y diagnóstico:

```bash
uv run --locked --extra warehouse secop-publication inspect --publication-id "$PUBLICATION_ID"
uv run --locked --extra warehouse secop-publication retry --publication-id "$PUBLICATION_ID" \
  --timeout-seconds "$PUBLICATION_BUILD_TIMEOUT_SECONDS" \
  --storage-budget-bytes "$PUBLICATION_STORAGE_BUDGET_BYTES"
```

El retry reutiliza las entradas capturadas y la expectativa original. Si deseas
abandonar un candidato privado inspeccionado, usa `discard --publication-id ID`.
Un lock impide descartar/sellar mientras otro proceso lo construye. Si el
contenido/catálogo cambia, descarta explícitamente y crea otro candidato;
no reescribas evidencia histórica ni fuerces la reutilización de IDs.

```bash
uv run --locked --extra warehouse secop-publication cleanup-preview
uv run --locked --extra warehouse secop-publication cleanup --publication-id "$OBSOLETE_ID"
```

La limpieza vuelve a comprobar elegibilidad y dependencias bajo el mismo lock
de activación. Elimina solo objetos registrados; una dependencia externa o una
espera de DDL impide la operación y revierte cualquier cambio parcial. Conserva
tombstones/auditoría saneada. Los controles usan espera de lock 5 segundos y
timeout de sentencia 30 segundos; reintenta tras diagnosticar contención. No
ejecutes borrados por patrones ni `CASCADE` contra schemas ajenos.

La ausencia o falla de scoring no retira analítica. Hasta que exista un productor
real, el estado es `ABSENT`; el helper puro `scoring_compatibility` exige el ID
analítico exacto y distingue ausencia, falla e incompatibilidad sin inventar scores.
