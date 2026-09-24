# Ingesta incremental - Bronze

Esta guía explica cómo extraer contratos SECOP II de Antioquia hacia local. La ingesta conserva las páginas tal como las entrega Socrata y mantiene los metadatos por separado. No limpia ni interpreta campos de negocio.

## Ejecución limitada

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

## Estructura local

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

## Lanes y checkpoint

El lane principal pagina por `ultima_actualizacion` e `id_contrato`, con una ventana de solapamiento de siete días por defecto. Una run completa puede avanzar el checkpoint hasta el límite superior solicitado, incluso cuando el rango no contiene filas.

Los argumentos, manifiestos y checkpoints usan UTC con zona explícita. Al construir SoQL, la ingesta quita únicamente el sufijo de zona  porque Socrata publica los valores sin zona.

Los registros con `ultima_actualizacion` nulo se consultan por separado mediante un rango explícito de `fecha_de_firma` y se ordenan por `id_contrato`. Este lane declara `captures_updates: false`: recupera registros dentro del rango de firma, pero no garantiza detectar modificaciones posteriores. Los registros sin ambas fechas tampoco se consideran cubiertos.

## Inspección y recuperación

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

## Fallos y diagnóstico

Los eventos se escriben como líneas JSON con identificador de run, evento, lane, página, intento, duración y conteos aplicables. No contienen respuestas completas, cabeceras sensibles ni credenciales.

Los códigos 429 y 5xx, los timeouts y los errores de conexión se reintentan de forma finita con backoff. Los demás 4xx terminan inmediatamente. Un fallo de red, esquema o escritura deja el checkpoint anterior intacto y conserva el staging verificable cuando existe.

Una columna crítica ausente o un tipo incompatible bloquea el cierre. Una columna adicional se reporta como drift, pero no entra silenciosamente en la proyección Bronze.

## Verificación local

La suite automatizada no usa red:

```bash
make quality
```

La comprobación contra Socrata debe ser manual, con un rango reciente y límites pequeños. Si la cuota pública rechaza persistentemente esas solicitudes, configura un token como variable de entorno.  
