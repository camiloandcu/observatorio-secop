# Perfil de la fuente SECOP II

Este documento resume una observación controlada del conjunto público `SECOP II - Contratos Electrónicos` (`jbjy-vk9h`). No es una descarga del histórico nacional ni una garantía sobre datos futuros.

## Alcance observado

- Fecha UTC: `2026-09-23T02:26:45+00:00`.
- Filtro: `departamento='Antioquia'`.
- Registros reportados para Antioquia: 576,332.
- Muestra de perfilado: 100 registros.

## Decisiones verificadas

La llave natural es `id_contrato`. En 576,332 registros de Antioquia se observaron 0 nulos y 0 grupos duplicados.

El watermark es `ultima_actualizacion` y se desempata con `id_contrato`. Tiene 342,475 valores no nulos de 576,332 registros (40.58% nulos), con rango `2017-09-13T00:00:00.000` a `2026-09-18T00:00:00.000`. La fuente lo describe como la última actualización del contrato; no declara zona horaria.

## Cobertura territorial

El departamento se filtra por `departamento` con el valor exacto `Antioquia`. Los valores observados para el Valle de Aburrá son:

| Municipio canónico | Valor observado | Registros |
|---|---|---:|
| Medellín | `Medellín` | 284,011 |
| Barbosa | `Barbosa` | 3,300 |
| Bello | `Bello` | 17,880 |
| Caldas | `Caldas` | 6,641 |
| Copacabana | `Copacabana` | 4,269 |
| Envigado | `Envigado` | 25,826 |
| Girardota | `Girardota` | 5,095 |
| Itagüí | `Itagui` | 7,487 |
| La Estrella | `La Estrella` | 11,639 |
| Sabaneta | `Sabaneta` | 19,014 |

La comparación territorial normaliza mayúsculas, tildes y espacios para detectar variantes; las consultas reproducibles conservan siempre los valores exactos publicados por la fuente.

## Perfil de columnas

Las métricas siguientes pertenecen únicamente a la muestra.

| Campo API | Etiqueta | Tipo declarado | Tipos observados | Nulos | Cardinalidad |
|---|---|---|---|---:|---:|
| `ciudad` | Ciudad | `text` | text: 100 | 0 | 16 |
| `codigo_de_categoria_principal` | Codigo de Categoria Principal | `text` | text: 100 | 0 | 63 |
| `codigo_entidad` | Codigo Entidad | `number` | number: 100 | 0 | 32 |
| `codigo_proveedor` | Codigo Proveedor | `text` | text: 100 | 0 | 87 |
| `condiciones_de_entrega` | Condiciones de Entrega | `text` | text: 100 | 0 | 6 |
| `departamento` | Departamento | `text` | text: 100 | 0 | 1 |
| `descripcion_del_proceso` | Descripcion del Proceso | `text` | text: 100 | 0 | 88 |
| `descripcion_documentos_tipo` | Descripcion Documentos Tipo | `text` | text: 100 | 0 | 1 |
| `destino_gasto` | Destino Gasto | `text` | text: 100 | 0 | 2 |
| `dias_adicionados` | Dias adicionados | `number` | number: 100 | 0 | 2 |
| `direcci_n_de_ejecuci_n_del_contrato` | Dirección de ejecución del contrato | `text` | text: 100 | 0 | 43 |
| `documento_proveedor` | Documento Proveedor | `text` | text: 100 | 0 | 84 |
| `documentos_tipo` | Documentos Tipo | `text` | text: 100 | 0 | 1 |
| `domicilio_representante_legal` | Domicilio Representante Legal | `text` | text: 100 | 0 | 34 |
| `duraci_n_del_contrato` | Duración del contrato | `text` | text: 100 | 0 | 2 |
| `el_contrato_puede_ser_prorrogado` | El contrato puede ser prorrogado | `text` | text: 100 | 0 | 2 |
| `entidad_centralizada` | Entidad Centralizada | `text` | text: 100 | 0 | 2 |
| `es_grupo` | Es Grupo | `text` | text: 100 | 0 | 2 |
| `es_pyme` | Es Pyme | `text` | text: 100 | 0 | 2 |
| `espostconflicto` | EsPostConflicto | `text` | text: 100 | 0 | 2 |
| `estado_contrato` | Estado Contrato | `text` | text: 100 | 0 | 7 |
| `fecha_de_fin_del_contrato` | Fecha de Fin del Contrato | `calendar_date` | calendar_date: 89 | 11 | 47 |
| `fecha_de_firma` | Fecha de Firma | `calendar_date` | calendar_date: 85 | 15 | 31 |
| `fecha_de_inicio_del_contrato` | Fecha de Inicio del Contrato | `calendar_date` | calendar_date: 89 | 11 | 25 |
| `fecha_de_notificaci_n_de_prorrogaci_n` | Fecha de notificación de prorrogación | `calendar_date` | calendar_date: 19 | 81 | 9 |
| `fecha_fin_liquidacion` | Fecha Fin Liquidacion | `calendar_date` | calendar_date: 42 | 58 | 30 |
| `fecha_inicio_liquidacion` | Fecha Inicio Liquidacion | `calendar_date` | calendar_date: 42 | 58 | 28 |
| `g_nero_representante_legal` | Género Representante Legal | `text` | text: 100 | 0 | 3 |
| `habilita_pago_adelantado` | Habilita Pago Adelantado | `text` | text: 100 | 0 | 2 |
| `id_contrato` | ID Contrato | `text` | text: 100 | 0 | 100 |
| `identificaci_n_representante_legal` | Identificación Representante Legal | `text` | text: 100 | 0 | 32 |
| `justificacion_modalidad_de` | Justificacion Modalidad de Contratacion | `text` | text: 100 | 0 | 7 |
| `liquidaci_n` | Liquidación | `text` | text: 100 | 0 | 2 |
| `localizaci_n` | Localización | `text` | text: 100 | 0 | 18 |
| `modalidad_de_contratacion` | Modalidad de Contratacion | `text` | text: 100 | 0 | 8 |
| `n_mero_de_cuenta` | Número de cuenta | `text` | text: 100 | 0 | 62 |
| `n_mero_de_documento_ordenador_de_pago` | Número de documento Ordenador de Pago | `text` | text: 100 | 0 | 1 |
| `n_mero_de_documento_ordenador_del_gasto` | Número de documento Ordenador del gasto | `text` | text: 100 | 0 | 15 |
| `n_mero_de_documento_supervisor` | Número de documento supervisor | `text` | text: 100 | 0 | 21 |
| `nacionalidad_representante_legal` | Nacionalidad Representante Legal | `text` | text: 100 | 0 | 3 |
| `nit_entidad` | Nit Entidad | `number` | number: 100 | 0 | 31 |
| `nombre_del_banco` | Nombre del banco | `text` | text: 100 | 0 | 30 |
| `nombre_entidad` | Nombre Entidad | `text` | text: 100 | 0 | 32 |
| `nombre_ordenador_de_pago` | Nombre Ordenador de Pago | `text` | text: 100 | 0 | 1 |
| `nombre_ordenador_del_gasto` | Nombre ordenador del gasto | `text` | text: 100 | 0 | 16 |
| `nombre_representante_legal` | Nombre Representante Legal | `text` | text: 100 | 0 | 87 |
| `nombre_supervisor` | Nombre supervisor | `text` | text: 100 | 0 | 22 |
| `objeto_del_contrato` | Objeto del Contrato | `text` | text: 100 | 0 | 88 |
| `obligaci_n_ambiental` | Obligación Ambiental | `text` | text: 100 | 0 | 2 |
| `obligaciones_postconsumo` | Obligaciones Postconsumo | `text` | text: 100 | 0 | 1 |
| `orden` | Orden | `text` | text: 100 | 0 | 3 |
| `origen_de_los_recursos` | Origen de los Recursos | `text` | text: 100 | 0 | 1 |
| `pilares_del_acuerdo` | Pilares del Acuerdo | `text` | text: 100 | 0 | 1 |
| `presupuesto_general_de_la_nacion_pgn` | Presupuesto General de la Nacion – PGN | `number` | number: 100 | 0 | 1 |
| `proceso_de_compra` | Proceso de Compra | `text` | text: 100 | 0 | 84 |
| `proveedor_adjudicado` | Proveedor Adjudicado | `text` | text: 100 | 0 | 87 |
| `puntos_del_acuerdo` | Puntos del Acuerdo | `text` | text: 100 | 0 | 1 |
| `rama` | Rama | `text` | text: 100 | 0 | 3 |
| `recursos_de_credito` | Recursos de Credito | `number` | number: 100 | 0 | 1 |
| `recursos_propios` | Otros Recursos (Especie, Privados, Cooperación, Propios Entidades Autónomas) | `number` | number: 100 | 0 | 1 |
| `recursos_propios_alcald_as_gobernaciones_y_resguardos_ind_genas_` | Recursos Propios (Alcaldías y Gobernaciones) | `number` | number: 100 | 0 | 1 |
| `referencia_del_contrato` | Referencia del Contrato | `text` | text: 100 | 0 | 100 |
| `reversion` | Reversion | `text` | text: 100 | 0 | 1 |
| `saldo_cdp` | Saldo CDP | `number` | number: 100 | 0 | 64 |
| `saldo_vigencia` | Saldo Vigencia | `number` | number: 100 | 0 | 3 |
| `sector` | Sector | `text` | text: 100 | 0 | 10 |
| `sistema_general_de_participaciones` | Sistema General de Participaciones | `number` | number: 100 | 0 | 1 |
| `sistema_general_de_regal_as` | Sistema General de Regalías | `number` | number: 100 | 0 | 1 |
| `tipo_de_contrato` | Tipo de Contrato | `text` | text: 100 | 0 | 7 |
| `tipo_de_cuenta` | Tipo de cuenta | `text` | text: 100 | 0 | 3 |
| `tipo_de_documento_ordenador_de_pago` | Tipo de documento Ordenador de Pago | `text` | text: 100 | 0 | 1 |
| `tipo_de_documento_ordenador_del_gasto` | Tipo de documento Ordenador del gasto | `text` | text: 100 | 0 | 2 |
| `tipo_de_documento_supervisor` | Tipo de documento supervisor | `text` | text: 100 | 0 | 2 |
| `tipo_de_identificaci_n_representante_legal` | Tipo de Identificación Representante Legal | `text` | text: 100 | 0 | 3 |
| `tipodocproveedor` | TipoDocProveedor | `text` | text: 100 | 0 | 3 |
| `ultima_actualizacion` | Ultima Actualizacion | `calendar_date` | calendar_date: 50 | 50 | 45 |
| `urlproceso` | URLProceso | `url` | object: 100 | 0 | 84 |
| `valor_amortizado` | Valor Amortizado | `number` | number: 100 | 0 | 1 |
| `valor_de_pago_adelantado` | Valor de pago adelantado | `number` | number: 100 | 0 | 1 |
| `valor_del_contrato` | Valor del Contrato | `number` | number: 100 | 0 | 88 |
| `valor_facturado` | Valor Facturado | `number` | number: 100 | 0 | 14 |
| `valor_pagado` | Valor Pagado | `number` | number: 100 | 0 | 13 |
| `valor_pendiente_de` | Valor Pendiente de Amortizacion | `number` | number: 100 | 0 | 1 |
| `valor_pendiente_de_ejecucion` | Valor Pendiente de Ejecucion | `number` | number: 100 | 0 | 83 |
| `valor_pendiente_de_pago` | Valor Pendiente de Pago | `number` | number: 100 | 0 | 83 |

## Diferencias y limitaciones

- El modelo analítico usa el concepto `source_updated_at`; la fuente real publica `ultima_actualizacion`. El contrato conserva el nombre real y deja el renombrado para una transformación posterior.
- La fuente publica `ciudad`, mientras el modelo objetivo habla de municipios. Además, `Itagüí` aparece como `Itagui`; el mapeo queda explícito y no modifica el dato de origen.
- `ultima_actualizacion` tiene 233,857 nulos. Por tanto, una ingesta futura necesitará una estrategia adicional para esos registros.
- La unicidad y los conteos son evidencia fechada, no garantías sobre cambios futuros de la fuente.

## Reproducción y controles

```bash
make profile-source       # vuelve a observar y genera los tres artefactos
make validate-source      # valida contrato y fixture sin red
make check-source-live    # comprueba columnas críticas contra la fuente
make test                 # ejecuta todas las pruebas offline
```

La comprobación en vivo está separada de CI. Un fallo de red nunca reemplaza el contrato, la fixture ni este reporte.
