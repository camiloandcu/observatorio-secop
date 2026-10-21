# Identidad de plataforma entre capas

Los códigos publicados por SECOP identifican entidades y proveedores dentro del dataset
`jbjy-vk9h`. Los nombres son etiquetas: un cambio de nombre conserva identidad y dos
códigos distintos no se fusionan por compartir nombre. Esto no prueba identidad jurídica
ni permite inferir conductas irregulares.

## Evidencia y reglas

El perfil acotado del **19-10-2026, 14:29 UTC** consultó metadatos y 100 contratos de
Antioquia ordenados por `id_contrato`, mediante dos solicitudes HTTP. Ambos códigos
fueron cadenas de dígitos en las 100 observaciones: 33 códigos de entidad y 100 de
proveedor. No se observaron ausencias, etiquetas ausentes, variantes de etiqueta por
código ni etiquetas compartidas por códigos diferentes. El valor contractual de la
muestra sumó exactamente **3.220.726.505,00 COP**.

Es una muestra ordenada, no una estimación representativa del universo ni una prueba
de estabilidad longitudinal. No se conservaron filas fuente en Git. Las consultas,
límites, fecha, tipos declarados, métricas y digest de la muestra están en el
[reporte agregado](../contracts/evidence/identity_profile_2026-10-19.json).

Las [reglas versionadas](../contracts/identity_rules.yaml) aceptan `[0-9]+`, recortan
solo espacios exteriores y conservan ceros iniciales. No convierten códigos a enteros,
no eliminan sufijos decimales y no sustituyen nombres o documentos por códigos.
Los tokens `0`, `-1`, `N/A`, `NA`, `NULL`, `NO DEFINIDO` y `NO APLICA` se tratan como
placeholders por política conservadora; no se observaron en esta muestra. Otras formas
quedan desconocidas y requieren evidencia antes de ampliar la regla.

Los tokens numéricos de códigos se capturan exactamente antes de cualquier conversión
a float. Por ejemplo, `00123` se conserva si llegó como texto y `123.0` no se transforma
en `123`. El código crudo permanece trazable.

Para generar otro reporte agregado sin sobrescribir el contrato ni el reporte previo:

```bash
uv run --locked --extra dev --extra data --extra warehouse secop-source-profile identity \
  --output data/profiles/identity-new-observation.json
```

El comando requiere una ruta nueva, aplica la proyección permitida y respeta 100 filas
de muestra, 250 por respuesta, 20 intentos HTTP **incluidos reintentos**, 2 MiB por
respuesta y 20 segundos por intento. Una falla no reemplaza evidencia válida anterior.

## Estado desconocido y cobertura

Silver y PostgreSQL conservan códigos crudos/usables, etiquetas, estado y motivo por
rol. Los motivos son:

| Motivo | Significado |
|---|---|
| `LEGACY_PROJECTION` | La observación original no solicitaba identidad de plataforma |
| `MISSING_CODE` | Proyección nueva con código ausente, nulo o vacío |
| `PLACEHOLDER_CODE` | Token excluido por la política de placeholders |
| `INVALID_CODE` | Forma que no satisface la regla revisada |

Un nombre ausente no elimina un código válido. Un nombre presente no completa un código
faltante. Los contratos desconocidos se conservan y comparten un miembro desconocido
estable por dimensión; los motivos individuales siguen en el hecho.

`manifest.identity_coverage` informa, por rol, contratos conocidos/desconocidos, valor
COP exacto, motivos y cobertura por conteo y valor sobre contratos aceptados y
deduplicados. Las sumas se concilian con ese denominador. Una población vacía o valor
total cero produce cobertura nula con motivo, no un porcentaje inventado. Las variantes
de etiqueta se diagnostican separadamente. Estas métricas no usan el denominador de
observaciones del perfilado.

## Compatibilidad y migración

El [registro de compatibilidad](../contracts/secop_compatibility.yaml) enumera las
combinaciones exactas de dataset, bytes/hash del contrato y proyección. Las revisiones
anteriores se conservan en `contracts/revisions/`; no se acepta un hash distinto por
parecer compatible ni se editan manifiestos históricos.

| Entrada | Tratamiento |
|---|---|
| Bronze `secop-contracts-v1` registrado | Campos de identidad no disponibles; motivo legacy |
| Bronze `secop-contracts-v2` registrado | Clasificación por reglas de código; nombres opcionales |
| Silver esquema 1 sin identidad | Verificación física original, luego adaptación en memoria |
| Silver esquema 2 con identidad | Verificación estricta de esquema, regla, perfil y estados |
| Revisión/proyección/esquema no registrado | Rechazo explícito antes de modificar PostgreSQL |

Se mantienen los lectores temporales INT96/microsegundos soportados, sin pérdida de
precisión y con inventario exacto. La adaptación no cambia bytes, hashes ni punteros.
Si un bundle legacy no registró el hash fuente, el campo adaptado queda nulo: no se
inventa procedencia.

La primera carga migra columnas y registra revisión relacional 2 en
`silver.load_audit_schema`, dentro del lock y la transacción existentes. Esto ocurre
antes de decidir un no-op. La migración por sí sola no agrega una activación ni cambia
los registros inmutables de carga. Un error revierte esquema, filas y metadatos juntos.

Las llaves de entidad/proveedor cambian de la identidad anterior por nombre/desconocido
a dataset + rol + código. Sus sustitutas conocidas usan `platform_code_key_v1`;
`contract_key`, `contract_sk` y las reglas de otras dimensiones permanecen estables.
Cada dimensión selecciona una etiqueta no vacía con orden determinístico y conserva
exactamente un miembro por identidad. No es SCD2.

En la primera actualización, detén consumidores/escritores anteriores, conserva un
respaldo y los bundles previos, verifica/carga una candidata autoritativa con alcance
o reintenta la versión histórica ya activa, y reconstruye dbt explícitamente. Las nuevas
construcciones sin alcance son vistas previas; consulta [cobertura y recuperación](population-recovery.md).

```bash
uv run --locked --extra dev --extra data --extra warehouse secop-warehouse verify
make load-warehouse
make dbt-build DBT_ARGS=--full-refresh
```

Los builds siguientes pueden ser incrementales. La reconstrucción de dbt no garantiza
publicación Gold atómica global. Antes de reanudar consumidores, valida tests, llaves,
conteos y valores. Para recuperar, restaura el bundle previo con el lector compatible
y reconstruye dbt; un binario anterior que desconoce esquema 2 no es un rollback válido.

## Recarga acotada de identidad

Fija el manifiesto inmutable de un baseline verificado. Prepara localmente un archivo
JSON con IDs **que ya pertenecen a ese baseline**, únicos y dentro del límite declarado.
La recarga solicita filas completas por IDs exactos, incluso con fechas antiguas o
nulas; no consulta documentos personales, contactos ni cuentas bancarias.

```bash
BASELINE_MANIFEST='data/silver/secop_contracts/versions/<baseline-version>/manifest.json'
uv run --locked --extra dev --extra data --extra warehouse secop-ingest refresh-identity \
  --baseline "$BASELINE_MANIFEST" --ids data/identity-refresh-ids.json \
  --max-rows 100 --page-size 50 --max-requests 20
```

Los manifiestos registran `purpose=identity_refresh`, baseline/hash, IDs/digest, límites,
páginas, ausencias y progreso separado. No reinician ni adelantan el checkpoint
incremental. Un límite de intentos agotado deja `truncated_recoverable`, devuelve código
2 y requiere repetir los mismos argumentos con `--resume <run_id>`. El límite de intentos
es por invocación; el progreso conserva el total acumulado. Una run completa reanudada
reutiliza su evidencia sin consultar de nuevo; una adquisición nueva puede observar
cambios reales de la fuente.

Construye Silver con **todas** las dependencias Bronze del baseline y la run de recarga
completa, identificadas en sus manifiestos. No publiques solo la run de recarga. La
construcción independiente con `run` produce una vista previa y exige igualdad del
conjunto completo de llaves, también con `--run-id` explícito. Para una población
acotada que hereda refrescos históricos, cada nodo original conserva esa comprobación
contra su propia base; la población exterior prueba por separado conservación y nuevas
admisiones. El warehouse verifica ese respaldo antes de abrir PostgreSQL. Cantidades
iguales no sustituyen conjuntos de llaves iguales.

Una respuesta completa sin un ID conserva el contrato del baseline; una respuesta
truncada no habilita el candidato. La selección de versión sigue usando actualización
fuente y observación/linaje. Una fila refrescada más antigua puede perder: su código no
se injerta en la observación ganadora. El manifiesto informa identidades completadas,
todavía desconocidas y contratos con cambios en atributos de negocio. Los cambios de
proyección/hash por sí solos no prueban cambios de negocio.

Esta operación completa identidad en una población fijada; no ofrece recuperación de
toda la población ni modifica las políticas generales de reemplazo de snapshots.
