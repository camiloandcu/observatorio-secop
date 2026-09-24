# dbt

Este proyecto transforma el snapshot Silver cargado en PostgreSQL en un modelo estrella de contratos. Incluye cinco dimensiones, `fact_contract`, pruebas de integridad y documentación navegable.

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

Consulta [la guía del modelo estrella](../docs/star-schema.md) antes de cargar o reconstruir datos.
