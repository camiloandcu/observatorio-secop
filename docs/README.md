# Documentación

Este directorio reúne las guías públicas del Observatorio SECOP. Elige el punto de entrada según lo que necesites hacer.

## Empezar

- [Primeros pasos](getting-started.md): requisitos para WSL2, instalación de herramientas e inicio rápido.
- [Desarrollo](development.md): flujo con `uv`, comandos de calidad, CI, estructura y dependencias.
- [Operación local](operations.md): servicios, health checks, logs, diagnóstico, recuperación y limpieza.

## Verificación

- [Perfil de la fuente SECOP II](data-source-profile.md): esquema observado, cobertura territorial, llave, watermark y limitaciones verificadas.
- [Identidad de plataforma](platform-identity.md): códigos y cobertura de entidad/proveedor, compatibilidad versionada, migración y recarga acotada.
- [Ingesta incremental Bronze](bronze-ingestion.md): extracción limitada, estado, manifiestos, reanudación y diagnóstico.
- [Cobertura y recuperación acotada](population-recovery.md): población por firma, presupuestos, evidencia, adopción y restauración.
- [Transformación y calidad Silver](silver-quality.md): esquema tipado, deduplicación, rechazos, controles y publicación atómica.
- [Modelo estrella con dbt](star-schema.md): carga Silver en PostgreSQL, dimensiones, hecho, pruebas, reconstrucción y catálogo.

La documentación sobre indicadores, priorización, API y dashboard se añadirá con las historias que implementen esas capacidades.
