# Adquisición y transformación

Bronze conserva la evidencia fuente; Silver tipifica y valida sin alterar esos originales. El agotamiento incremental no demuestra cobertura por firma.

## Ingesta incremental - Bronze

Esta guía explica cómo extraer contratos SECOP II de Antioquia hacia local. La ingesta conserva las páginas tal como las entrega Socrata y mantiene los metadatos por separado. No limpia ni interpreta campos de negocio.

El agotamiento incremental no demuestra cobertura de una población por firma. Para recuperar contratos antiguos o con actualización nula sin mover el checkpoint, usa [recuperación acotada](05_poblacion_y_recuperacion.md).

La proyección actual añade `codigo_entidad`, `codigo_proveedor` y `proveedor_adjudicado`.
Cada run nueva registra revisión fuente y proyección; las anteriores conservan sus bytes.
Las combinaciones soportadas y el comando `refresh-identity`, con progreso independiente
del checkpoint, se explican en [Identidad de plataforma](04_identidad.md).

### Ejecución limitada

Instala el entorno y ejecuta un rango UTC explícito:

```bash
make install
make ingest-bronze \
  BRONZE_FROM=2026-09-01T00:00:00Z \
  BRONZE_TO=2026-09-02T00:00:00Z \
  BRONZE_MAX_ROWS=100 \
  BRONZE_PAGE_SIZE=50
```

Cada solicitud incluye un límite. El máximo de filas se comparte entre los lanes incremental y de respaldo. Si se alcanza antes de agotar el rango, la run queda marcada como truncada y no avanza el checkpoint.

El token de aplicación de Socrata es opcional. Si necesitas usarlo, pon `SOCRATA_APP_TOKEN` en un .env.

### Estructura local

Los datos quedan bajo `data/bronze/secop_contracts/`:

```text
staging/<run_id>/
  pages/<lane>-<secuencia>.json
  page-state.json
runs/<run_id>/
  pages/<lane>-<secuencia>.json
  manifest.json
state/ingestion.sqlite
locks/jbjy-vk9h.lock
```

- `staging/` conserva páginas completas de runs interrumpidas.
- `runs/` contiene runs publicadas con sus bytes originales y hashes SHA-256.
- `manifest.json` registra parámetros, consultas, conteos, cursores, reintentos, drift y transición del checkpoint.
- `ingestion.sqlite` mantiene el checkpoint, las runs comprometidas y el índice lógico por contrato y hash de contenido.

No edites `ingestion.sqlite`, `page-state.json` ni los manifiestos manualmente.

### Lanes y checkpoint

El lane principal pagina por `ultima_actualizacion` e `id_contrato`, con una ventana de solapamiento de siete días por defecto. Una run completa puede avanzar el checkpoint hasta el límite superior solicitado, incluso cuando el rango no contiene filas.

Los argumentos, manifiestos y checkpoints usan UTC con zona explícita. Al construir SoQL, la ingesta quita únicamente el sufijo de zona  porque Socrata publica los valores sin zona.

Los registros con `ultima_actualizacion` nulo se consultan por separado mediante un rango explícito de `fecha_de_firma` y se ordenan por `id_contrato`. Este lane declara `captures_updates: false`: recupera registros dentro del rango de firma, pero no garantiza detectar modificaciones posteriores. Los registros sin ambas fechas tampoco se consideran cubiertos.

### Inspección y recuperación

Consulta el estado sin modificarlo:

```bash
make inspect-bronze
```

El resultado muestra runs en staging, runs publicadas, observaciones lógicas y el checkpoint actual. Para revisar una run, abre su `manifest.json` y comprueba `status`, `range_complete`, `truncated`, `counts`, `pages` y `checkpoint_candidate`.

Si una ejecución se interrumpe, toma el `run_id` del log o del directorio de staging y reanúdala con los mismos parámetros efectivos:

```bash
uv run --locked --extra dev secop-ingest run \
  --from 2026-09-01T00:00:00Z \
  --to 2026-09-02T00:00:00Z \
  --max-rows 100 \
  --page-size 50 \
  --resume <run_id>
```

La reanudación verifica contrato, parámetros y hashes, reutiliza las páginas completas y continúa desde el último cursor confirmado. Un archivo temporal incompleto no se acepta como página. Un hash alterado o parámetros diferentes detienen la recuperación sin cambiar el checkpoint.

Antes de una run nueva, los lotes publicados que quedaron en estado preparado se reconcilian automáticamente con SQLite. No hace falta adelantar ni retroceder el checkpoint a mano.

Para eliminar únicamente staging no comprometido:

```bash
make clean-bronze-staging
```

Este comando no elimina runs publicadas ni modifica el checkpoint.

### Fallos y diagnóstico

Los eventos se escriben como líneas JSON con identificador de run, evento, lane, página, intento, duración y conteos aplicables. No contienen respuestas completas, cabeceras sensibles ni credenciales.

Los códigos 429 y 5xx, los timeouts y los errores de conexión se reintentan de forma finita con backoff. Los demás 4xx terminan inmediatamente. Un fallo de red, esquema o escritura deja el checkpoint anterior intacto y conserva el staging verificable cuando existe.

Una columna crítica ausente o un tipo incompatible bloquea el cierre. Una columna adicional se reporta como drift, pero no entra silenciosamente en la proyección Bronze.

### Verificación local

La suite automatizada no usa red:

```bash
make quality
```

La comprobación contra Socrata debe ser manual, con un rango reciente y límites pequeños. Si la cuota pública rechaza persistentemente esas solicitudes, configura un token como variable de entorno.

## Transformación y calidad - Silver

Silver convierte Bronze a contratos tipificados, normalizados y deduplicados. Cada versión contiene datos válidos, rechazados, métricas y resultados de controles. Una falla crítica conserva evidencia, pero no cambia la versión publicada.

Las construcciones nuevas con `run` son vistas previas sin selección autoritativa. Para publicar y cargar una población con alcance verificado, sigue [Cobertura y recuperación acotada](05_poblacion_y_recuperacion.md).

### Ejecutar e inspeccionar

Instala las dependencias y procesa todas las runs Bronze publicadas:

```bash
make install
make build-silver
make inspect-silver
```

Para construir una versión desde una selección cerrada, repite `--run-id`:

```bash
make build-silver SILVER_ARGS="--run-id <run_id_1> --run-id <run_id_2>"
```

La ejecución no consulta la red, no necesita secretos y no modifica páginas, manifiestos ni checkpoints Bronze. Antes de iniciar verifica estado `committed`, dataset, contrato, hashes y conteos de todas las páginas.

### Raíz y configuración compartidas

Silver y el warehouse usan `data/silver/secop_contracts` por defecto. Ejecuta ambos desde la raíz del repositorio. Un `SILVER_ROOT` exportado prevalece sobre `silver_root` del YAML seleccionado; vacío o solo espacios es un error. Sin esa variable se usa el YAML. No se busca automáticamente otra carpeta si falta el puntero.

`--config` selecciona el archivo y se escribe antes del subcomando:

```bash
uv run --locked --extra data secop-silver --config config/silver_quality.yaml run
```

Los comandos Python no cargan `.env` automáticamente. Para usarlo, expórtalo en la misma shell antes de construir y cargar:

```bash
set -a
source .env
set +a
make build-silver
uv run --locked --extra warehouse secop-warehouse verify
make load-warehouse
```

Revisa un `.env` anterior: puede conservar `SILVER_ROOT=data/silver`. Ajusta esa línea a la ubicación elegida o exporta explícitamente la raíz existente para ambas etapas. No copies ni muevas bundles para resolver una diferencia de configuración. La [guía del warehouse](02_warehouse_y_metricas.md) explica la compatibilidad de versiones publicadas.

### Esquema y conversiones

El esquema completo está definido en `src/observatorio_secop/processing/silver/schemas.py`. Sus grupos principales son:

- identidad: `contract_key`, `source_dataset_id`, `source_contract_id`;
- contrato: referencia, proceso, entidad, estado, categoría, tipo y modalidad;
- territorio: valores crudos, claves normalizadas, nombres canónicos y banderas de Medellín y Valle de Aburrá;
- fechas: valores crudos, fechas de firma/inicio/fin y actualización fuente;
- valor: `contract_value_raw` y `contract_value_cop` como `decimal(20,2)`;
- trazabilidad: hash del payload, run, página, lane, secuencia, ordinal y fecha de observación Bronze;
- calidad: advertencias y `signing_year` como partición.

Las fechas aceptan exactamente `yyyy-MM-dd'T'HH:mm:ss.SSS`. La política histórica `silver-v1` exige medianoche para las fechas de negocio. La política `silver-v2-calendar-date` valida el timestamp completo y conserva como `date` el día escrito por la fuente, independientemente de su hora, sin conversión de zona. En ambas políticas se conserva el valor crudo y se rechazan fechas imposibles o formatos no admitidos. La fuente no declara zona horaria para `ultima_actualizacion`: se usa UTC como contexto reproducible de cálculo, pero el valor crudo y esa limitación permanecen en el manifiesto.

El escritor usa Parquet `TIMESTAMP_MICROS` para datos válidos y rechazados. `source_updated_at` conserva el valor de reloj fuente sin afirmar una zona conocida; `bronze_fetched_at_utc` representa un instante UTC. La anotación UTC que Spark puede incluir en Parquet no cambia esa distinción ni desplaza los valores a hora local.

Los montos no aceptan moneda, separadores de miles, exponentes ni negativos. Los decimales posteriores a la segunda posición solo pueden ser ceros: `18600000.000000` se representa exactamente como `18600000.00` y conserva su valor crudo; `1.001` se rechaza. No hay redondeo, imputación ni sustitución silenciosa.

### Identidad y deduplicación

La identidad de entidad/proveedor usa códigos de plataforma y nombres como etiquetas.
El esquema 2 conserva código crudo/usable, estado/motivo y contrato/proyección de origen;
las runs legacy soportadas quedan desconocidas sin inferir códigos del nombre. El
[contrato de identidad](04_identidad.md) detalla cobertura por conteo/valor,
compatibilidad, recarga acotada y migración. Una falta de código conserva el contrato;
una inconsistencia entre código, estado y regla es crítica.

`contract_key` es SHA-256 de la versión de regla, `jbjy-vk9h` y `id_contrato` normalizado únicamente con Unicode NFC y espacios exteriores. El identificador crudo siempre se conserva. Identificadores nulos se rechazan y cualquier llave asociada con más de un identificador crudo se reporta como colisión crítica.

Los duplicados exactos comparten llave y hash canónico del payload. Entre versiones distintas gana, en orden:

1. `source_updated_at` no nulo;
2. mayor `source_updated_at`;
3. observación Bronze más reciente;
4. `run_id`, página, ordinal y hash como desempates totales.

Una llegada posterior con timestamp fuente anterior no sustituye una versión más nueva. Si todas las versiones tienen actualización nula, el linaje Bronze rompe el empate, sin afirmar que demuestra la última actualización real de la fuente.

### Territorio

El catálogo local versionado contiene los 125 municipios de Antioquia, alias explícitos y los diez municipios del Valle de Aburrá. La comparación ignora caso, tildes y espacios repetidos. El alias observado `Itagui` produce el canónico `Itagüí` sin cambiar `municipality_raw`.

Un departamento fuera de Antioquia se rechaza. Un municipio desconocido permanece como fila válida con `municipality_status=UNKNOWN`, nombre canónico nulo y advertencia `UNKNOWN_MUNICIPALITY`; no se aplica coincidencia aproximada ni se inventa una equivalencia.

### Rechazos, advertencias y controles

Una observación rechazada aparece una sola vez aunque tenga varios motivos. Guarda códigos ordenados, campo, razón, valor crudo limitado, payload proyectado y linaje Bronze. Los códigos iniciales son:

- `IDENTIFIER_NULL`;
- `AMOUNT_NULL`, `AMOUNT_NEGATIVE`, `AMOUNT_INVALID_FORMAT`, `AMOUNT_SCALE_EXCEEDED`, `AMOUNT_OVERFLOW`;
- `DATE_INVALID`, `DATE_INCONSISTENT`;
- `DEPARTMENT_OUT_OF_SCOPE`.

Pandera valida con el backend PySpark el esquema estricto, tipos, nulidad, unicidad, montos, fechas y territorio. El orden se compara directamente contra el `StructType` versionado debido a una limitación del backend Pandera 0.23 con esquemas amplios. Las métricas agregadas validan colisiones, conciliación y tasa de rechazo.

Son críticas las fallas de evidencia Bronze, esquema, campos obligatorios, unicidad, montos, fechas, territorio, colisiones, conciliación, salida vacía y una tasa superior al 2%. El umbral exacto del 2% pasa. Una advertencia queda visible pero no bloquea por sí sola.

La conciliación es:

```text
input_rows = output_rows + rejected_rows + exact_duplicates + superseded_versions
duplicate_rows = exact_duplicates + superseded_versions
```

### Publicación, recuperación y rollback

La estructura local ignorada por Git es:

```text
data/silver/secop_contracts/
  staging/<silver_version_id>/
  invalid/<silver_version_id>/
  versions/<silver_version_id>/
    data/signing_year=<YYYY|unknown>/*.snappy.parquet
    rejected/*.snappy.parquet
    quality/results.json
    manifest.json
  current.json
```

El bundle se escribe primero en staging, se relee y verifica. Si pasa, un rename en el mismo filesystem crea la versión inmutable. Solo una candidata autoritativa completa y compatible puede actualizar `current.json`; una vista previa permanece disponible por versión. Si falla, el candidato se mueve a `invalid/` y la versión válida anterior permanece intacta.

Repetir las mismas entradas, contrato, configuración, revisión de esquema/identidad,
registro de compatibilidad y perfil del escritor devuelve el mismo `silver_version_id`.
Si ya existe un bundle idéntico, la operación es idempotente. Cada run se valida contra
su revisión registrada y conserva su hash real, aunque difiera del contrato actual.

El manifiesto nuevo registra `writer_format_version=silver-parquet-v2`; ese perfil también participa en la identidad de versión. Un cambio de formato genera otra versión incluso con un YAML antiguo sin cambios, sin modificar reglas de negocio, llaves ni archivos previos. Los manifiestos anteriores sin ese campo siguen siendo legibles.

Para restaurar una versión registrada, usa la identidad destino y la identidad actual esperada con `restore`, según la [guía de recuperación](05_poblacion_y_recuperacion.md). La mera existencia de un bundle válido no autoriza su activación.

Para eliminar únicamente staging incompleto:

```bash
make clean-silver-staging
```

El comando no elimina versiones válidas, candidatos inválidos, Bronze ni checkpoints. Antes de limpiar, usa `make inspect-silver` y conserva el manifiesto de cualquier fallo que necesite diagnóstico.

### Verificación offline

```bash
make quality
```

Las fixtures cubren duplicados exactos, versiones tardías nuevas y antiguas, cursor nulo, monto inválido, fecha incoherente, identificador nulo, colisión inducida, municipio desconocido, umbral de rechazo, publicación fallida, idempotencia y rollback. Los datos generados permanecen fuera de Git.
