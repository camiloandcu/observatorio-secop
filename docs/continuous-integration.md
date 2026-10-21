# Integración continua y seguridad

El workflow ejecuta calidad e integración real en un job y dos controles de seguridad
independientes. Un resultado aprobado exige los tres: las pruebas offline sin PostgreSQL
no acreditan integración. El tipado actual cubre únicamente
`src/observatorio_secop/warehouse`; formato, lint y pruebas tienen alcance más amplio.

## Reproducir integración estricta

Instala uv 0.12.0, Python 3.12 y Java 17. `make install` usa el lock y los extras
`dev`, `data` y `warehouse`. El workflow usa Ubuntu 24.04 y PostgreSQL 16.4, con
acciones fijadas por SHA e imagen por digest. Los parches efectivos quedan en la
evidencia; los cambios de versiones requieren repetir los controles afectados.

Algunas pruebas eliminan schemas completos. Crea una base **dedicada y desechable**,
sin volumen de trabajo ni datos reales. Este contenedor expone solo el loopback local:

```bash
make install
docker run --detach --rm --name secop-ci-test-postgres \
  --publish 127.0.0.1:55432:5432 \
  --env POSTGRES_HOST_AUTH_METHOD=trust \
  --env POSTGRES_DB=secop_ci_local postgres:16.4-alpine
docker exec secop-ci-test-postgres pg_isready -U postgres -d secop_ci_local
export WAREHOUSE_TEST_DSN='host=127.0.0.1 port=55432 dbname=secop_ci_local user=postgres'
export CI_DISPOSABLE_DB_HOST=127.0.0.1
export CI_DISPOSABLE_DB_PORT=55432
export CI_DISPOSABLE_DB_NAME=secop_ci_local
export SPARK_LOCAL_IP=127.0.0.1
make ci-preflight
make ci
```

Espera la respuesta saludable de `pg_isready`. El preflight exige destino explícito
coincidente, nombre `secop_ci_*`, Java 17 y runtimes de Spark/dbt. No infiere la base
ni usa la configuración de trabajo cuando falta el DSN. El nombre por sí solo no
hace desechable una base: el operador debe crear el contenedor dedicado mostrado.
La autenticación `trust` se limita a ese servicio efímero de pruebas; no es configuración
para un servidor compartido.

El runner ejecuta `make quality` serialmente. Las pruebas construyen Bronze sintético,
Silver con Spark real, verifican su inventario, cargan PostgreSQL y ejecutan dbt.
Comprueban precisión temporal, rutas, dinero decimal, identidad, población,
reconstrucción, restauración A → B → A y fallos que conservan la versión válida.
La restauración incluye reconstruir dbt y conciliar filas, monto y versión de A.
No ejecutes dos suites relacionales contra la misma base ni uses ejecución paralela
para estas fixtures: comparten schemas y salidas dbt.

Al terminar, conserva solo los reportes necesarios y detén el contenedor dedicado:

```bash
unset WAREHOUSE_TEST_DSN CI_DISPOSABLE_DB_HOST CI_DISPOSABLE_DB_PORT CI_DISPOSABLE_DB_NAME
docker stop secop-ci-test-postgres
```

`make quality` sigue siendo útil sin base para desarrollo offline; sus skips se
muestran, pero no sustituyen `make ci` para verificar integración.

## Inventario y evidencia de ejecución

`config/ci_integration.json` fija los casos críticos revisados y las familias cuya
colección actual también es obligatoria. Cuando cambia un identificador, revisa el
cambio semántico y actualiza el inventario; no lo regeneres automáticamente para
hacer desaparecer un caso fallido. Las nuevas pruebas de familias críticas también
se exigen aunque no estuvieran en la lista inicial.

`make ci` produce exclusivamente estos reportes subibles:

| Archivo | Contenido |
|---|---|
| `reports/ci/outcomes.json` | Identificadores recolectados/deseleccionados y resultados/duración por fase |
| `reports/ci/summary.json` | Conteos por familia, faltantes, resultado del gate y versiones |
| `reports/ci/resources.json` | Duración, SHA, CPU/RAM/disco y muestras de memoria/base |

No contienen mensajes completos de excepción, DSN ni perfiles. Los motivos de
omisión se expresan mediante categorías saneadas. Una prueba crítica ausente,
deseleccionada, omitida, xfail/xpass, fallida o con error hace fallar el gate.
Un reporte vacío, malformado o incompleto también falla. Se conserva el error
original de pytest; producir diagnósticos no lo convierte en éxito.

Para revisar un reporte ya generado sin repetir la suite:

```bash
uv run --locked --extra dev --extra data --extra warehouse \
  python scripts/ci_integration.py gate --report reports/ci/outcomes.json
```

## Escaneos de seguridad

Los controles requieren acceso a las descargas de herramientas y al servicio público
de advisories, sin una cuenta nueva ni tokens cloud. Los datos de prueba permanecen
sintéticos y no se consulta SECOP. Ejecuta:

```bash
make security-secrets
make security-dependencies
# ambos, serialmente:
make security
```

Gitleaks analiza el historial Git obtenido, incluidos secretos eliminados del HEAD,
con salida redactada y versión/descarga verificadas. Un checkout superficial no
acredita cobertura histórica; CI obtiene el historial completo. El escaneo se limita
a Git y no recorre indiscriminadamente archivos privados locales no versionados.

pip-audit analiza una exportación locked de **todos** los extras declarados:
`dev`, `data`, `warehouse`, `orchestration`, `ml` y `api`. No instala esos servicios
para auditarlos. El reporte declara intérprete/plataforma, marcadores y exclusiones;
un resultado no demuestra seguridad de otros ecosistemas, plataformas o fechas.
Las herramientas están fijadas en la configuración y el lock de auditoría separado.

Los códigos de salida distinguen éxito, hallazgos y fallo de herramienta/política.
Hallazgos sin excepción aprobada, historial incompleto, errores de descarga,
exportación o advisories bloquean; no se convierten en “sin vulnerabilidades”.
Los reportes permitidos son `reports/security/secrets.json` y
`reports/security/dependencies.json`, con metadatos y diagnósticos saneados.
No publiques los archivos temporales sin redactar.

Las excepciones son explícitas y requieren revisión humana. Para vulnerabilidades:
advisory y alias, paquete/versión, aplicabilidad, mitigación, responsable, aprobación y
vencimiento. Deben coincidir exactamente; una excepción vencida o de otro paquete
no permite el hallazgo. Para secretos solo se admiten falsos positivos identificados
por fingerprint/regla/ruta y justificados; un secreto usable necesita remoción y
rotación en su ámbito correspondiente. No se crea una baseline automática ni se
actualizan dependencias como efecto del escaneo. Una migración incompatible requiere
revisar su alcance antes de ejecutarla.

## Permisos y privacidad

Los jobs usan `contents: read`, checkout sin persistencia de credenciales, runners
efímeros y acciones inmutables. No se usa ejecución privilegiada de código de PR,
OIDC ni tokens de escritura. El servicio PostgreSQL es propio del job.

Las cachés del workflow están desactivadas. Solo se sube la allowlist de reportes
saneados anterior, con retención de siete días. Quedan fuera variables locales,
perfiles, datos, Parquet, rechazos, dumps, salidas completas de dbt, workspace y
planificación privada. Los controles del índice también rechazan archivos privados
forzados mediante `git add -f`; `.gitignore` no basta por sí solo.

## Presupuesto y diagnóstico

Los límites iniciales son 30 minutos para calidad/integración y 10 minutos por
escáner. Se contrastan con una ejecución completa real; un límite configurado no
es una medición. El entorno nuevo de Actions con cachés desactivadas permite medir
la instalación y la suite desde un runner frío. La evidencia de cada ejecución
identifica SHA, versiones y tamaño lógico de fixtures.

El runner registra preflight/calidad y duraciones de fases pytest; el log de Actions
muestra además instalación y pasos de seguridad. Muestrea cada segundo RSS por
grupos Python/JVM/PostgreSQL visibles en `/proc` y `pg_database_size` de la base
exclusiva, además del espacio de workspace/disco. Es una observación del entorno:
RSS puede duplicar páginas compartidas, perder picos breves e incluir otros procesos;
PostgreSQL en otro namespace puede no ser visible en RSS. El tamaño de la base no
incluye WAL ni capas del contenedor. La memoria disponible declarada describe el
host visible, no garantiza una cuota de memoria del job.

Si falla preflight, corrige runtimes/destino y no ejecutes fixtures destructivas.
Si falla el gate, consulta los identificadores faltantes y resultados; no silencies
skips. Si hay timeout, registra la etapa, recursos y datos conocidos antes de
revisar el margen o la topología. Si falla un escáner, distingue un hallazgo de un
error de herramienta/red y repite solo después del diagnóstico o corrección.
No amplíes límites ni agregues excepciones silenciosamente.

El build y smoke test de imagen API se incorporarán con el servicio API; este
workflow no presenta los marcadores de ese servicio como una API funcional.
