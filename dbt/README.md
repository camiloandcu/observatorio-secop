# dbt

Este proyecto transforma el snapshot Silver cargado en PostgreSQL en un modelo estrella de contratos. Incluye cinco dimensiones, `fact_contract`, cinco marts Gold, consultas gobernadas, pruebas de integridad y documentación navegable.

La configuración local vive fuera de Git:

```bash
mkdir -p .dbt
cp dbt/profiles.yml.example .dbt/profiles.yml
set -a
source .env
set +a
make dbt-debug
make dbt-build
make dbt-docs
```

Consulta [la guía del modelo estrella](../docs/star-schema.md) antes de cargar o reconstruir datos y el [contrato de indicadores Gold](../docs/gold-analytics.md) para métricas, filtros, calidad y recuperación. Los comandos `make dbt-*` verifican los hashes del catálogo y la política antes de ejecutar dbt.
