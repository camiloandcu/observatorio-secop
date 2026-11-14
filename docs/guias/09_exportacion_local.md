# Exportación local verificable

El exportador prepara un paquete privado de una publicación analítica retenida: Silver válido minimizado, once vistas Gold y metadata. No crea recursos ni envía datos a AWS. Las consultas, permisos y costos cloud tienen una operación y aprobación separadas.

Ejecuta estos comandos desde el checkout Linux/WSL con las dependencias `warehouse` instaladas. Usa las variables PostgreSQL de la [guía de publicación](../referencia/03_publicacion.md) y una cuenta lectora; no copies contraseñas a comandos, archivos públicos ni capturas. El exportador usa transacciones de solo lectura y no cambia la publicación activa.

```bash
uv sync --locked --extra dev --extra warehouse
uv run --locked --extra warehouse secop-cloud-export build \
  --publication-id PUBLICATION_ID \
  --silver-root data/silver/MI_BUNDLE \
  --output reports/cloud-export

uv run --locked --extra warehouse secop-cloud-export verify \
  --package reports/cloud-export/HASH_EXPORTACION
```

El directorio de salida debe ser privado (`0700`). `export_id` es el SHA-256 canónico de publicación, Silver/scope, contrato y receta del escritor (código, PyArrow y opciones Parquet). El CLI lo calcula; `--export-id` permite comprobar un hash esperado. La identidad selecciona una salida inmutable; no selecciona otra publicación ni amplía permisos. Nunca uses una base de trabajo como destino de pruebas de integración.

## Entrada y fidelidad

Se fija un descriptor una sola vez. El bundle se verifica con los controles compartidos de inventario, timestamps, identidad, población y calidad; Silver, hash y scope deben coincidir con la publicación. Se compara exactamente cada llave y los valores compartidos con el detalle, sin reconstruir el warehouse. Las columnas proceden exclusivamente de [cloud_export.yaml](../../contracts/cloud_export.yaml); cambios de esquema se rechazan.

El paquete conserva dinero `DECIMAL(20,2)`, fechas calendario y nulos. Los numeric sin precisión declarada se representan como texto decimal exacto. Los timestamps se guardan como texto con seis decimales: `utc-instant-us-v1` termina en `Z`; `wall-clock-us-v1` conserva el reloj sin asignarle zona. La convención UTC de observaciones almacenadas sin zona se declara por columna. JSONB se obtiene como texto del servidor, conservando números exactos y JSON null frente a SQL NULL. `text[]` admite una dimensión con límite inferior 1, vacío o NULL; no se aplana otra estructura.

La validación de contenido se aplica también al verificar Parquet: estructuras JSON cerradas, enlaces SECOP aprobados y ausencia de rutas/contactos restringidos. Si una etiqueta permitida contiene un contacto, se rechaza el paquete. La única excepción implementada, `supplier-name-null-v1`, requiere aprobación humana y un recibo privado fijado a publicación/hash Silver, hash del texto esperado y exactamente una llave de dimensión más dos de detalle. `--label-null-approval ARCHIVO` autoriza únicamente esas tres celdas de `supplier_name` a NULL; se conservan todas las filas, IDs y montos. El manifiesto declara política/objetivos/conteos y la identidad los incorpora; el texto y su hash permanecen fuera del paquete. Cambiar una celda, faltar un objetivo o encontrar otro contacto aborta. No generes recibos para nuevas celdas por inferencia.

## Paquete y comprobaciones

Cada relación contiene Parquet Snappy; Silver, hecho y detalle se particionan por año/mes de firma. Los directorios usan `signing_year_partition` y `signing_month_partition`, para conservar la columna analítica `signing_month` sin colisión. Las fechas ausentes tienen partición `unknown`.

`manifest.json` declara identidades, contrato y tipos/codecs, inventario SHA-256, bytes, filas, nulos, rangos de fechas, sumas exactas y digest de multiconjunto por relación. `schema.sql` contiene DDL parametrizado y `reconciliation.sql` entrega las trece consultas candidatas del digest completo para Athena engine 3; el CLI no ejecuta esos archivos. Los resultados esperados son los conteos/digests del manifiesto. Antes de confiar en esa SQL se requiere probar en Athena las fixtures de codecs, nulos/UTF8/decimales, vacío y duplicados dentro de la operación cloud aprobada. La construcción compara todos los valores de la proyección declarada con cada fila/columna decodificada de Parquet mediante un multiconjunto exacto en scratch privado, además de comprobar llaves/FK y digests.

El algoritmo `logical-digest-v1` enmarca cada celda como `N;` o `V<bytes_utf8>:<valor>;`, hashea cada fila y luego la concatenación de hashes hex ordenados, conservando duplicados. Un digest se apoya en la resistencia a colisiones de SHA-256. El manifiesto autocontenido no autentica autoría: conserva su hash en un recibo confiable fuera del paquete antes de transportarlo.

Dos construcciones independientes con las mismas entradas y receta generan los mismos archivos e inventario. Repetir un ID existente verifica la salida como `unchanged`; una diferencia produce conflicto y no sobrescribe. El verificador rechaza archivos extra/ausentes/alterados, symlinks, escapes, schema o contenido no admitido. `verified` acredita las comprobaciones locales; no prueba consultas cloud ni aceptación humana.

## Límites y recuperación

| Recurso | Límite |
|---|---:|
| Tiempo global desde validación Silver hasta sellado | 300 s |
| Filas combinadas | 1.000.000 |
| Filas por lote | 1.000 |
| Bytes codificados por fila / lote | 8 MiB /16 MiB |
| RSS incremental observado por proceso | 256 MiB |
| Paquete completo | 80 MiB /400 archivos |
| Scratch privado temporal | 512 MiB |
| Admisión de row group descomprimido | 96 MiB |
| Concatenación de hashes por relación | 8 MiB |

Lectura, escritura y verificación independiente se ejecutan en procesos supervisados. La memoria se observa durante la ejecución y entre lotes; no se garantiza un pico absoluto entre muestras. Los timeouts SQL se limitan al plazo restante y el supervisor termina el proceso al agotar recursos. No hay reintentos automáticos ni recorte de población. Los límites no se amplían mediante flags del CLI.

Los errores propios son `EXPORT_INPUT_CONFLICT`, `EXPORT_UNSUPPORTED_SCHEMA`, `EXPORT_LIMIT`, `EXPORT_PACKAGE_CONFLICT` y `EXPORT_INCOMPLETE`; expiración/denegación/inaccesibilidad conservan códigos `PUBLICATION_*`. La salida diagnóstica evita valores y credenciales. Un rechazo puede requerir revisar el contrato o recuperar la conexión antes de repetir una operación completa.

Los intentos fallidos quedan en `.staging-<export-id>-<aleatorio>` sin ser una salida sellada. Inspecciona únicamente el intento propio y su diagnóstico privado antes de borrar esa ruta exacta. No elimines por comodines otros staging, paquetes, locks ni datos de origen. El scratch de validación se elimina solo con salida del proceso confirmada. Si la cancelación no puede confirmarse, se conserva privado y el CLI devuelve error; el diagnóstico de cancelación se intenta sin convertir su escritura en permiso de limpieza. Revisa PID y la ruta exacta de request del worker antes de recuperar ese temporal; no supongas que un PID antiguo sigue perteneciendo al mismo proceso. Una ejecución fallida no invalida un paquete anterior.

Funciones SQL consultadas el14-11-2026: [Athena](https://docs.aws.amazon.com/athena/latest/ug/functions.html), [binarios/SHA-256 de Trino484](https://trino.io/docs/current/functions/binary.html) y [agregación](https://trino.io/docs/current/functions/aggregate.html). Son referencias de diseño; no acreditan ejecución en Athena.
