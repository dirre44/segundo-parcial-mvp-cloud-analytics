# Log de decisiones — MVP Segundo Parcial

## Patrón: Lambda

Igual que en el proyecto final: los maestros (batch, CSV) y los eventos de uso (streaming, JSONL) son fuentes estructuralmente distintas desde el origen. Se ingestan por caminos separados a Bronze y convergen recién ahí. Kappa habría forzado tratar los CSV batch como streaming sin beneficio real.

## Particionado

- `billing_monthly` → partición por `month`: el patrón de consulta natural ("facturación de tal mes") se beneficia directamente.
- `usage_events` (Bronze y Silver) → partición por `usage_month` (no por `usage_date`): con ~43k filas, particionar por día genera demasiadas carpetas chicas para el volumen de datos, sin beneficio de consulta proporcional. Mensual reduce la cantidad de particiones sin perder la capacidad de filtrar por fecha (la columna `usage_date` sigue existiendo, solo que no es partición física).
- `customers_orgs` / `users` → sin particionar: son tablas de dimensión chicas (80 y 800 filas); particionarlas no aporta y solo agrega archivos pequeños innecesarios.
- Mart Gold `org_daily_usage_by_service` → sin particionar en Parquet: es el resultado final, ya agregado y chico; la "partición" real para consulta ocurre en Cassandra vía la `PRIMARY KEY`.

## Checkpoint de streaming — persistente, no efímero

A diferencia de una primera iteración del proyecto (donde el checkpoint se borraba en cada corrida para simplificar reprocesos completos), en este MVP el checkpoint vive en Drive y **no se borra**. Esto es intencional: la consigna pide explícitamente demostrar "checkpointing habilitado", y la única forma de demostrarlo de verdad es mostrando que una segunda corrida, sin archivos nuevos, no reprocesa nada (`numInputRows = 0`). El notebook corre el stream dos veces seguidas y compara.

## Watermark de 65 días

Los 120 archivos de `usage_events_stream` están deliberadamente desordenados respecto al timestamp del evento (un archivo "más nuevo" en nombre puede tener timestamps más viejos que uno anterior). Structured Streaming procesa archivos en orden de descubrimiento, no por contenido. Un watermark corto (horas/días) descartaría como "demasiado tarde" una fracción enorme de eventos legítimos, solo por ese desorden artificial. Se eligió un watermark que cubre todo el histórico (~60 días) para no perder datos válidos, documentando que en producción (con latencia de arribo real de segundos/minutos) correspondería un watermark mucho más ajustado.

## Reglas de calidad — quarantine vs. flag

No todas las reglas de calidad tienen la misma severidad:

- **`event_id` nulo** → quarantine (sin esto no se puede deduplicar ni trazar el evento).
- **`unit` nulo cuando `value` existe** → quarantine (es una lectura inconsistente: hay una magnitud pero no se sabe en qué unidad, no se puede agregar con confianza).
- **`cost_usd_increment < -0.01`** → solo flag (`is_cost_anomaly`), la fila se mantiene. Un monto negativo puede representar un crédito o ajuste legítimo, no necesariamente un error — descartarlo de plano perdería información de negocio real.

El dataset sintético usado resultó tener 0 violaciones de las reglas 1 y 3 (es bastante limpio en integridad estructural). Para no depender de que el dataset productivo tenga ejemplos "a mano", el notebook incluye una prueba con 3 filas armadas a propósito que verifica que el filtro de quarantine detecta correctamente los casos problemáticos.

## Carga a AstraDB — `foreachPartition`, no colectar al driver

La consigna pide explícitamente cargar "desde Spark (`foreachBatch` o conector)". Se evaluaron tres opciones:

1. **Colectar a `pandas` en el driver** (lo que se hizo en una iteración anterior del proyecto): simple, pero no es realmente "desde Spark" — se pierde el paralelismo y todo el trabajo de escritura recae en un solo proceso.
2. **`spark-cassandra-connector` oficial**: es la opción más "nativa", pero su compatibilidad con Spark 4.0 (versión que Colab trae preinstalada al momento de este parcial) es incierta, y requiere matchear versiones de Scala/Spark vía `--packages`. El riesgo de romper la entrega por un problema de versiones no se justificaba para el alcance de este MVP.
3. **`DataFrame.foreachPartition()` + `cassandra-driver`** (elegido): cada partición de Spark abre su propia conexión a Cassandra y escribe sus filas directamente. Es una escritura distribuida real, sin dependencias de jars externos, y funciona igual de bien en modo local (Colab) que en un clúster real.

## Rendimiento de la carga — `execute_concurrent_with_args` dentro de cada partición

La primera versión de `write_partition_to_astra` hacía un `session.execute()` por fila, secuencial (espera la respuesta del servidor antes de mandar la siguiente). Con 11.050 filas eso medía ~50 escrituras/seg — varios minutos en total. Se cambió a `execute_concurrent_with_args` (hasta 100 requests en vuelo por partición): cada partición sigue abriendo su propia conexión (sigue siendo una escritura distribuida "desde Spark"), pero deja de esperar request por request.

## Claves de Cassandra (`org_daily_usage_by_service`)

`PRIMARY KEY (org_id, usage_date, service)`, con `CLUSTERING ORDER BY (usage_date DESC, service ASC)`. Diseño query-first: el patrón de acceso principal es "traerme el uso/costo de tal organización en tal rango de fechas" (Query 1), que se resuelve con un filtro por partición (`org_id`) y rango en el primer clustering column (`usage_date`) — sin `ALLOW FILTERING`. La Query 2 (punto exacto org+fecha+servicio) es un caso particular del mismo patrón.

## Idempotencia

No hay lógica de "upsert manual": en Cassandra, escribir una fila con la misma `PRIMARY KEY` sobreescribe (last-write-wins). Como la `PRIMARY KEY` de la tabla coincide exactamente con la clave de agrupación del mart de Gold (`org_id, usage_date, service`), recargar el mismo mart nunca duplica. Se demuestra cargando dos veces y comparando `COUNT(*)` antes/después.
