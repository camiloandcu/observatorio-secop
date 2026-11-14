# Explorar el dashboard

El reporte de Power BI Desktop permite explorar el último estado observado de contratos SECOP II de entidades registradas en Antioquia. Antes de empezar, pide al operador un proyecto con Import validado; para reconstruirlo, sigue la [guía técnica BI](../../bi/README.md#reconstruct-a-local-project). No requiere Power BI Service y no ofrece acceso interactivo público.

## Leer el alcance

Comprueba en el encabezado población, periodo de firma, cobertura, publicación y fechas de observación e Import. Un refresh posterior de la fuente puede cambiar periodos anteriores. El reporte describe la publicación importada, no el estado actual de SECOP II.

Los valores son totales contractuales observados en COP nominales, no pagos. El municipio corresponde al registro de la entidad, no al lugar de ejecución. Las muestras no representan toda la contratación de Antioquia. El reporte no ofrece scoring ni determina irregularidades.

## Elegir entidad y periodo

1. En **Panorama**, lee contratos, valor, mediana y tendencia. Usa **Entidad** y **Mes de firma** para conservar la referencia entre páginas; seleccionar una barra solo afecta su página.
2. El modo de firma admite todas las fechas, intervalo semiabierto `[inicio, fin)` o **Solo sin firma**. Un mes seleccionado excluye firmas ausentes: limpia el mes antes de revisar esas filas. Los filtros no recuperan contratos fuera del alcance importado.
3. Usa **Gráficos / Tablas** para consultar la alternativa tabular. **Ver contratos del mes** abre el detalle mensual; vuelve para elegir otro mes.
4. En **Contratos**, selecciona una fila y usa **Ver contrato**. **Volver a la lista** regresa al origen; entidad y periodo sincronizados permanecen, aunque debas seleccionar nuevamente la fila.
5. **Restablecer** limpia selecciones y restituye las vistas iniciales sin cambiar la publicación.

## Comparar entidades y proveedores

En **Entidades y proveedores**, revisa ranking y cobertura. **Proveedor destacado** muestra sus contratos y participación sin reducir la referencia usada para HHI. No promedies HHI ni sumes medianas; las medidas se recalculan desde contratos únicos bajo los filtros admitidos.

HHI necesita una sola entidad y cobertura de identidad compatible con la política. Si no está disponible, lee el motivo: desconocido, vacío y cero válido tienen significados distintos. Los proveedores sin código permanecen como desconocidos y no se convierten en un proveedor real. Consulta las [reglas de métricas](../referencia/02_warehouse_y_metricas.md#filtros-y-cálculo).

## Consultar evidencia y calidad

En el detalle, revisa referencia, entidad/proveedor, monto, fechas, advertencias y contexto de filtros. El enlace fuente abre el proceso de compra cuando su URL está permitida; si falta o no se admite, se muestra como no disponible.

**Calidad** separa evidencia global de la publicación, contratos de la referencia y contratos del proveedor destacado. Los rechazos globales no cambian con filtros contractuales que no pueden atribuírseles. Un control histórico no evaluado permanece no disponible. **Publicación / Diagnóstico** alterna las vistas sin capturar filtros. Los [granos técnicos de calidad](../../bi/README.md#quality-control-grains-and-filters) documentan cada denominador.

## Teclado y actualización

**Ctrl+F6** cambia entre lienzo y pestañas; **Tab** recorre controles y **Ctrl+flecha derecha** entra a un visual. **Enter** selecciona y **Esc** sale. **Alt+Shift+F11** abre la tabla accesible del visual. En Desktop 2.159.1231.0 en edición, Tab puede quedar dentro de la cabecera del primer visual; **Shift+Tab** permite recorrer los visuales en orden inverso. Es un límite observado, no una garantía para otras versiones.

Si falla **Home → Refresh**, conserva el último Import válido y lee su fecha. El Import anterior no conoce el fallo nuevo. Consulta el [procedimiento de recuperación](../../bi/README.md#refresh-and-recovery); no cambies credenciales ni definiciones para ocultarlo.

## Evidencia disponible

Las [capturas de referencia](../../bi/README.md#reference-screens) muestran datos sintéticos: 13 contratos con entidades y proveedores inventados. Sirven para conocer el diseño, no para describir contratación real. La [validación 2026Q1](../../bi/README.md#real-data-check-and-its-limits) documenta por separado un Import con datos reales y sus límites. Las capturas y el video real de presentación siguen en preparación.
