# Transformación y calidad - Silver

Silver convierte Bronze a contratos tipificados, normalizados y deduplicados. Cada versión contiene datos válidos, rechazados, métricas y resultados de controles. Una falla crítica conserva evidencia, pero no cambia la versión publicada.

## Ejecutar e inspeccionar

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

## Esquema y conversiones

El esquema completo está definido en `src/observatorio_secop/processing/silver/schemas.py`. Sus grupos principales son:

- identidad: `contract_key`, `source_dataset_id`, `source_contract_id`;
- contrato: referencia, proceso, entidad, estado, categoría, tipo y modalidad;
- territorio: valores crudos, claves normalizadas, nombres canónicos y banderas de Medellín y Valle de Aburrá;
- fechas: valores crudos, fechas de firma/inicio/fin y actualización fuente;
- valor: `contract_value_raw` y `contract_value_cop` como `decimal(20,2)`;
- trazabilidad: hash del payload, run, página, lane, secuencia, ordinal y fecha de observación Bronze;
- calidad: advertencias y `signing_year` como partición.

Las fechas aceptan exactamente `yyyy-MM-dd'T'HH:mm:ss.SSS`. Las fechas de negocio deben representar medianoche antes de convertirse a `date`. La fuente no declara zona horaria para `ultima_actualizacion`: se usa UTC como contexto reproducible de cálculo, pero el valor crudo y esa limitación permanecen en el manifiesto.

Los montos no aceptan moneda, separadores de miles, exponentes ni negativos. Los decimales posteriores a la segunda posición solo pueden ser ceros: `18600000.000000` se representa exactamente como `18600000.00` y conserva su valor crudo; `1.001` se rechaza. No hay redondeo, imputación ni sustitución silenciosa.

## Identidad y deduplicación

`contract_key` es SHA-256 de la versión de regla, `jbjy-vk9h` y `id_contrato` normalizado únicamente con Unicode NFC y espacios exteriores. El identificador crudo siempre se conserva. Identificadores nulos se rechazan y cualquier llave asociada con más de un identificador crudo se reporta como colisión crítica.

Los duplicados exactos comparten llave y hash canónico del payload. Entre versiones distintas gana, en orden:

1. `source_updated_at` no nulo;
2. mayor `source_updated_at`;
3. observación Bronze más reciente;
4. `run_id`, página, ordinal y hash como desempates totales.

Una llegada posterior con timestamp fuente anterior no sustituye una versión más nueva. Si todas las versiones tienen actualización nula, el linaje Bronze rompe el empate, sin afirmar que demuestra la última actualización real de la fuente.

## Territorio

El catálogo local versionado contiene los 125 municipios de Antioquia, alias explícitos y los diez municipios del Valle de Aburrá. La comparación ignora caso, tildes y espacios repetidos. El alias observado `Itagui` produce el canónico `Itagüí` sin cambiar `municipality_raw`.

Un departamento fuera de Antioquia se rechaza. Un municipio desconocido permanece como fila válida con `municipality_status=UNKNOWN`, nombre canónico nulo y advertencia `UNKNOWN_MUNICIPALITY`; no se aplica coincidencia aproximada ni se inventa una equivalencia.

## Rechazos, advertencias y controles

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

## Publicación, recuperación y rollback

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

El bundle se escribe primero en staging, se relee y verifica. Si pasa, un rename en el mismo filesystem crea la versión inmutable y un reemplazo durable actualiza `current.json`. Los lectores solo deben seguir ese puntero. Si falla, el candidato se mueve a `invalid/` y la versión válida anterior permanece intacta.

Repetir las mismas entradas, contrato y configuración devuelve el mismo `silver_version_id`. Si ya existe un bundle idéntico, la operación es idempotente. Para apuntar a otra versión válida:

```bash
uv run --locked --extra dev --extra data secop-silver rollback --version <silver_version_id>
```

Para eliminar únicamente staging incompleto:

```bash
make clean-silver-staging
```

El comando no elimina versiones válidas, candidatos inválidos, Bronze ni checkpoints. Antes de limpiar, usa `make inspect-silver` y conserva el manifiesto de cualquier fallo que necesite diagnóstico.

## Verificación offline

```bash
make quality
```

Las fixtures cubren duplicados exactos, versiones tardías nuevas y antiguas, cursor nulo, monto inválido, fecha incoherente, identificador nulo, colisión inducida, municipio desconocido, umbral de rechazo, publicación fallida, idempotencia y rollback. Los datos generados permanecen fuera de Git.
