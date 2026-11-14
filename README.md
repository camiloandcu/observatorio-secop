# Observatorio SECOP

Pipeline de ingeniería de datos para convertir contratos electrónicos SECOP II de entidades registradas en Antioquia en analítica trazable. Permite explorar contratos, proveedores y concentración en Power BI Desktop, desde una publicación validada y versionada.

Desarrollado por **Camilo Cuellar** como proyecto de portafolio de ingeniería de datos. Contacto: [LinkedIn](https://www.linkedin.com/in/camiloandcu/).

## Qué puedes evaluar

- **Adquisición acotada:** páginas fuente y manifiestos inmutables, presupuestos de filas/solicitudes, checkpoints y reanudación explícita.
- **Calidad e identidad:** PySpark tipifica y deduplica, conserva códigos de entidad/proveedor y separa rechazos sin sustituir la última publicación válida por una candidata fallida.
- **Analítica reproducible:** PostgreSQL y dbt construyen estrella y cinco marts con fórmulas gobernadas, dinero decimal y linaje hasta la observación fuente.
- **Publicación coherente:** validación antes de activar, lectores de solo lectura fijados a una versión y restauración A → B → A.
- **Orquestación y BI:** Airflow ejecuta los comandos del pipeline; Power BI Desktop importa una publicación identificada y permite explorar contratos, proveedores, calidad y evidencia.

La [arquitectura implementada](docs/README.md#arquitectura-implementada) muestra cómo se conectan las capas. Los procedimientos y controles están en las [guías públicas](docs/README.md).

## Ver el reporte y su evidencia

La [guía del dashboard](docs/guias/05_dashboard.md) explica el recorrido: elegir entidad y periodo, comparar proveedores, interpretar HHI y abrir la evidencia de un contrato.

Las [capturas actuales](bi/README.md#reference-screens) usan **datos sintéticos**: 13 contratos con entidades y proveedores inventados. Muestran diseño e interacciones; sus cifras no describen contratación real.

La [validación con datos reales de 2026Q1](bi/README.md#real-data-check-and-its-limits) documenta 52.005 contratos publicados a partir de una adquisición acotada por firma en `[2026-01-01, 2026-04-01)`, y la conciliación del Import con SQL de esa publicación. La fuente es mutable: otra adquisición puede producir resultados distintos. Capturas reales y video de presentación están en preparación. El reporte se usa en Desktop; no hay acceso interactivo público.

## Ejecutar y verificar

Empieza con la [preparación local](docs/guias/01_inicio.md). Para reconstruir el flujo aislado y observar sus tareas, sigue la [guía del pipeline](docs/guias/04_pipeline.md). La [construcción BI](bi/README.md) explica cómo generar el proyecto y validar el Import.

La [guía de desarrollo y verificación](docs/guias/02_desarrollo_y_verificacion.md) reúne instalación, calidad, CI y seguridad. `make quality` puede omitir integración sin DSN; `make ci` requiere Java 17 y una base PostgreSQL dedicada y desechable. Los comandos de recuperación están en [operación](docs/guias/03_operacion.md) y las referencias por capa.

## Alcance y límites

Los montos son totales contractuales observados en **COP nominales**, no pagos ni gasto ejecutado. La geografía corresponde al **registro de la entidad**, no necesariamente al lugar de ejecución. Los periodos por firma describen el último estado observado y pueden revisarse.

La priorización con ML, la API FastAPI y la exportación acotada S3/Glue/Athena siguen pendientes. MLflow dispone de un entorno opcional; eso no acredita un modelo de priorización. No se presenta operación cloud permanente ni entrega integral del MVP.

Una señal de revisión no establece fraude, corrupción ni responsabilidad. Tampoco se han validado adopción ni ahorro de tiempo con usuarios.

El avance se consulta en [Issues](https://github.com/camiloandcu/observatorio-secop/issues) y en el [tablero del proyecto](https://github.com/users/camiloandcu/projects/1).
