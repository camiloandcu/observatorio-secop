# Observatorio SECOP

El Observatorio SECOP transforma registros públicos de contratos SECOP II de entidades registradas en Antioquia en información comprensible sobre contratación y prioridades de revisión humana. El valor analizado es el total contractual nominal observado en COP; la ubicación corresponde al registro de la entidad y no identifica necesariamente el lugar de ejecución o gasto.

## El problema

La información contractual está disponible, pero su volumen, variabilidad y dispersión dificultan responder preguntas básicas de manera consistente: cuánto se contrata, con quién, bajo qué modalidades, cómo cambia el comportamiento en el tiempo y dónde aparecen patrones atípicos.

Esa dificultad limita el seguimiento ciudadano y obliga a invertir demasiado tiempo en reunir, limpiar y reconciliar datos antes de poder analizarlos.

El flujo objetivo es el siguiente; las capacidades disponibles se detallan en el alcance actual:

```mermaid
flowchart TD
    A["<b>Problema de negocio</b><br/>'Hay demasiados contratos para revisar manualmente'"]
    B["<b>SECOP II</b><br/>Datos públicos disponibles en Socrata"]
    C["<b>Ingesta incremental acotada</b><br/>Adquirir observaciones según ventanas y límites explícitos"]
    D["<b>BRONZE</b><br/>Copia cruda y trazable que sirve como evidencia"]
    E["<b>PySpark</b><br/>Limpiar, tipificar, validar y deduplicar"]
    F["<b>SILVER</b><br/>Datos limpios, tipificados, validados y deduplicados"]
    G["<b>dbt + PostgreSQL</b><br/>Modelo de negocio"]
    H["<b>Priorización / MLflow</b><br/>Evaluar y versionar señales de revisión"]
    I["<b>Publicación analítica validada</b><br/>Estrella, indicadores y calidad"]
    J["<b>Power BI</b><br/>Análisis visual de los datos"]
    K["<b>FastAPI</b><br/>Consulta técnica de los datos"]
    L["<b>Features retrospectivas</b><br/>Referencias y elegibilidad versionadas"]
    M["<b>Scores y razones</b><br/>Compatibles con la publicación analítica"]
    N["<b>S3 / Glue / Athena</b><br/>Exportación acotada validada"]

    A --> B
    B --> C
    C --> D
    D --> E
    E --> F
    F --> G
    G --> I
    I --> J
    I --> K
    I --> L
    L --> H
    H --> M
    M --> J
    M --> K
    I --> N
```

Airflow orquestará los mismos comandos del pipeline. La analítica base seguirá disponible cuando no haya un modelo o scores compatibles. La etapa AWS será acotada y requiere autorización de acceso, recursos y costos.

## La solución

El proyecto construye una ruta reproducible que transforma datos de SECOP II en:

- información contractual depurada y trazable;
- indicadores comparables sobre valor nominal COP, volumen y concentración;
- vistas para explorar entidades, proveedores, modalidades, municipios de registro y periodos de firma;
- señales explicables que ayuden a priorizar contratos para revisión humana;
- medios de consulta mediante productos analíticos y una API documentada.

La priorización es una ayuda para ordenar la revisión. Una señal alta NO demuestra fraude, corrupción, incumplimiento ni responsabilidad legal.

## Alcance actual

El repositorio cuenta con una base local reproducible, ingesta Bronze incremental acotada, una capa Silver tipificada con PySpark, carga y restauración de snapshots en PostgreSQL y un modelo estrella con dbt. La identidad de entidades y proveedores se conserva entre estas capas, con estados desconocidos explícitos y trazabilidad.

Los cinco [marts e indicadores Gold](docs/gold-analytics.md) están implementados como candidatos verificables, con catálogo de métricas, filtros gobernados y evidencia de calidad. Su construcción local no activa acceso de consumidores.

La [publicación analítica](docs/analytical-publication.md) construye en aislamiento, valida antes de activar y ofrece lecturas fijadas a una versión, permisos restringidos y recuperación. La orquestación de negocio con Airflow, la priorización con MLflow, la API FastAPI, el dashboard Power BI y la exportación acotada a AWS están pendientes. Su avance puede seguirse en el [tablero del proyecto](https://github.com/users/camiloandcu/projects/1).

## Principios

- Trazabilidad desde el dato publicado hasta el resultado presentado.
- Criterios y fórmulas explícitos, reproducibles y verificables.
- Separación clara entre hechos observados, indicadores y señales de priorización.
- Uso responsable: las anomalías orientan preguntas, no producen acusaciones.
- Protección de secretos, datos innecesarios y artefactos internos.
- Costos y límites operativos visibles.

## Documentación

La [página de documentación](docs/README.md) organiza las guías según la necesidad:

- [Primeros pasos](docs/getting-started.md) para preparar y levantar el entorno local.
- [Guía de desarrollo](docs/development.md) para calidad, CI y estructura del repositorio.
- [Guía de operación](docs/operations.md) para salud, logs, recuperación y limpieza.
- [Transformación y calidad Silver](docs/silver-quality.md) para el esquema, reglas, rechazos y publicación local.
- [Modelo estrella con dbt](docs/star-schema.md) para la carga relacional, dimensiones, hecho, pruebas y catálogo.

Las historias y su avance se consultan en [GitHub Issues](https://github.com/camiloandcu/observatorio-secop/issues) y en el [tablero del proyecto](https://github.com/users/camiloandcu/projects/1).
