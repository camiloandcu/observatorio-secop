# Indicadores y marts Gold

Gold describe el último estado válido observado de contratos de entidades
registradas en Antioquia. El valor es total contractual observado en COP nominales;
no mide pagos. El municipio corresponde al registro de la entidad. Las series por
firma pueden revisar meses anteriores cuando llega otra observación; no reconstruyen
el conocimiento histórico.

## Construcción local

Instala las dependencias fijadas y configura una base de desarrollo dedicada según
[modelo estrella](star-schema.md). Conserva el bundle Silver verificado y la misma
configuración de carga durante toda la construcción:

```bash
make load-warehouse
make dbt-build
make dbt-test
make dbt-docs
```

La carga comprueba archivos, calidad y población antes de escribir. Los cinco marts
se construyen desde la misma versión Silver, hash de manifiesto y scope; incluyen
revisiones del catálogo y política. Una comprobación de identidad fallida invalida
el candidato. No ejecutes escritores/cargas concurrentes con dbt.

Los schemas actuales de dbt son fijos: `staging`, `intermediate` y `analytics`.
`DBT_SCHEMA` no crea aislamiento para estos modelos. Usa una base dedicada; no
concedas acceso de consumidores a estos candidatos mutables. La construcción local
no activa una publicación validada.

## Relaciones y significado

| Relación candidata | Grano | Uso |
|---|---|---|
| `analytics.mart_contract_detail` | Snapshot/scope y contrato único | Población canónica para detalle y recálculo |
| `analytics.mart_contract_overview` | Snapshot/scope, mes de firma, municipio, modalidad y estado | Conteo, total y distribución de valor |
| `analytics.mart_entity_monthly` | Snapshot/scope, entidad y mes de firma | Serie del último estado observado |
| `analytics.mart_supplier_concentration` | Snapshot/scope, entidad, mes y cohorte | HHI y cobertura de identidad de referencia |
| `analytics.mart_data_quality` | Snapshot/scope, control y grano declarado | Observaciones globales y diagnósticos atribuibles |

El detalle mantiene llaves estables hacia entidad, proveedor, municipio, modalidad
y fechas de la estrella. La relación es muchos contratos a una fila de dimensión;
los miembros desconocidos son explícitos. Los enlaces de rechazo no amplían el
grano del contrato. Los consumidores deben preservar versión/hash/scope en cada
relación y comprobar compatibilidad antes de unir resultados.

La cohorte base `ALL_VALID_OBSERVED` incluye todos los contratos válidos seleccionados,
incluso Borrador y En aprobación. El estado se conserva y normaliza por espacios y
mayúsculas/minúsculas, sin fusionar etiquetas con significado diferente. Estado
faltante o no perfilado no significa contrato adjudicado ni firmado.

El catálogo versionado [gold_metrics.yaml](../contracts/gold_metrics.yaml) fija
fórmulas, tipos, unidades, filtros y motivos de nulidad. La política inicial exige
100% de identidad conocida por conteo y valor para publicar HHI; es una decisión
conservadora de producto, no validación estadística. Cambiarla requiere revisión.

## Filtros y cálculo

Entidad, municipio registrado, modalidad, estado y periodo de firma se intersectan
sobre el mismo detalle. El intervalo de firma es semiabierto `[inicio, fin)`.
Las fechas ausentes tienen selección explícita; combinar esa selección con un
intervalo fechado es contradictorio y se rechaza. Los filtros o agrupamientos no
soportados fallan: no se omiten silenciosamente.

Las consultas gobernadas `gold_overview_query`, `gold_entity_monthly_query` y
`gold_concentration_query` se definen como macros dbt sobre una relación de detalle
confiable. Las interfaces posteriores deben consumir esas fórmulas y validar sus
parámetros. No admitas SQL ni nombres de relaciones recibidos de usuarios. Los
valores de ejecución deben enlazarse como parámetros; los identificadores de
relación pertenecen al constructor confiable.

Conteo es cantidad distinta de llaves contractuales. La suma conserva aritmética
decimal; p25, mediana y p75 usan interpolación lineal numérica. Selección vacía tiene
conteo y suma cero, distribución nula y motivo `NO_CONTRACTS`; un grupo con contratos
de valor cero conserva distribución cero. Los agregados mensuales no permiten
sumar medianas ni promediar HHI. Para otros periodos o filtros, recalcula desde el
detalle una sola vez por contrato.

Seleccionar un proveedor destaca sus contratos y su participación, conservando
la referencia de concentración. Para cada entidad/periodo/cohorte:

- `N_T`, `V_T`: contratos y valor de toda la referencia.
- `N_K`, `V_K`: contratos y valor con identidad de proveedor conocida.
- Coberturas: `N_K/N_T` y `V_K/V_T`, con denominador cero explícito.
- HHI: suma de cuadrados de `valor_proveedor/V_K`, escala 0–1.

Los proveedores desconocidos permanecen en la cobertura total y no forman un
proveedor ficticio. A=40, B=40 y desconocido=20 tiene cobertura monetaria 80% y HHI
nulo con la política inicial. A=40/B=40 sin desconocidos tiene HHI 0,5. Un proveedor
conocido elegible tiene HHI 1; no es una etiqueta de irregularidad.

Los motivos distinguen política ausente, selección vacía, entidad desconocida,
sin proveedores conocidos, valor total o conocido cero y cobertura insuficiente.
No transformes un resultado no calculable en cero. Seleccionar un proveedor no
modifica estos motivos ni el denominador de referencia.

## Calidad y linaje

La evidencia de calidad procede del manifiesto y archivos inventariados verificados,
no de los contadores de inserciones del warehouse. Su contabilidad es:

```text
observaciones de entrada = contratos de salida + observaciones rechazadas
                         + duplicados exactos + versiones válidas reemplazadas
observaciones válidas aceptadas = entrada - rechazadas
```

Los controles globales de observaciones conservan su población completa aunque
un consumidor filtre entidad o proveedor. Solo diagnósticos atribuibles a contratos
admiten filtros contractuales y denominador de contratos únicos. Los rechazos sin
identidad no se atribuyen por conjetura. Evidencia heredada ausente se presenta
`UNAVAILABLE_LEGACY` con valores nulos, nunca con ceros inventados.

Distingue cobertura de muestra/parcial/completa, truncamiento/completitud por lane,
actualización fuente y finalización del pipeline. Una firma antigua no demuestra
frescura fallida. El detalle conserva las condiciones `LATER_INVALID_OBSERVED`,
`TEMPORAL_ORDER_INDETERMINATE`, `NO_LATER_INVALID_OBSERVED` y `LEGACY_UNASSESSED`.
Una observación válida retenida tras otra inválida posterior no demuestra validez
de la última observación. El consumidor de features decide la elegibilidad; Gold
preserva esa evidencia sin cambiar el ganador Silver.

Los enlaces de proceso solo se ofrecen si cumplen la lista permitida de esquema y
host. Enlaces ausentes o rechazados quedan no disponibles. La lectura prevista
excluye payloads rechazados, rutas locales y rutas de páginas Bronze; conserva
identificadores, hashes y tiempos saneados para trazabilidad.

## Recuperación y contrato de consumo

Una falla de carga revierte contratos, población y calidad juntos. Repetir el mismo
snapshot activo es no-op; restaurar A después de B utiliza el procedimiento
[de recuperación](population-recovery.md), luego reconstruye dbt. Compara conteo,
suma decimal, versión/hash/scope y calidad con A original. Nunca edites manifiestos
ni borres auditoría para forzar una carga.

Si dbt falla, conserva el diagnóstico y considera el candidato inválido. Corrige y
repite el build; usa `make dbt-build DBT_ARGS=--full-refresh` cuando cambie el esquema
físico. Los modelos locales no constituyen recuperación atómica para lectores.

La publicación analítica posterior debe ofrecer estas cinco relaciones validadas,
sus claves/dimensiones y metadatos de identidad, población, observación y
catálogo/política. BI y API usarán roles de solo lectura sobre esa publicación.
La activación, schemas/roles concretos, denegación de escritura y candidatos,
retención, rollback y vinculación coherente de un refresh de varias consultas
pertenecen al mecanismo de publicación. Esta entrega no concede permisos ni prueba
ese lifecycle. BI debe conciliar sus medidas y filtros con estas consultas; API
debe parametrizar valores y rechazar filtros incompatibles; features debe conservar
las condiciones de observación e interpretar su elegibilidad por separado.
