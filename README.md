# Pipeline medallion de publicidad digital en Databricks (Bronze → Silver → Gold)

Pipeline de datos sobre datos sintéticos de AdTech (advertisers, campaigns, ads, impressions, clicks, conversions; 30 días de octubre 2026 más 2 días de noviembre) con ingesta `COPY INTO`, limpieza en Silver y 8 tablas Gold que responden preguntas de negocio. Proyecto de formación como data engineer en Databricks (TP integrador, UTN).

## Hallazgos clave

- **Los ids de eventos no son únicos entre archivos**: el generador reinicia el contador en cada archivo, así que impressions tiene **150.000 ids distintos para 160.000 eventos reales**. Deduplicar por `id` habría borrado 10.000 eventos; Silver usa un `event_key` (hash de todos los campos).
- **Bronze cargó las dimensiones varias veces**: 600 filas para 200 advertisers, 6.000 para 2.000 campañas y 1.500.000 para 500.000 ads (una carga por archivo aterrizado). Silver las deja en 1 fila por `id`.
- **El pipeline cierra de punta a punta**: 160.000 impresiones, 3.184 clics y 256 conversiones en Silver; CTR global **1,99%** y CVR global **8,04%**, los valores con los que se diseñó el generador (2% y 8%).
- **El 95% de las campañas (1.903 de 2.000) no tiene ninguna conversión** y concentra el **92,5%** del gasto ($6.084,46 de $6.576,23). Solo Retail recupera más de lo que gasta (ROAS **1,52**; global 0,89).

## Resumen ejecutivo

Trabajo de práctica para recorrer una arquitectura medallion completa en Databricks con Unity Catalog. Se generan datos crudos, se ingestan sin transformar a Bronze, se limpian y se les da integridad referencial en Silver, y se agregan en Gold para responder 8 preguntas de negocio.

## Problema

Los datos de origen traen duplicados, claves huérfanas, nulos, valores en cero e ids que se repiten entre archivos. Si se limpian al ingerir se pierde trazabilidad; si no se limpian, las métricas de negocio salen infladas o con atribuciones equivocadas (un clic puede apuntar a dos impresiones distintas con el mismo `id`).

## Dominio de negocio

Embudo clásico de publicidad digital:

```
Advertiser → Campaign → Ad → Impression → Click → Conversion
```

1. Un **advertiser** (empresa) quiere promocionar un producto.
2. Crea **campaigns** con objetivo, canal (Search, Social, Display, Video) y presupuesto diario.
3. Cada campaña tiene **ads** (creatividades: Banner, Video, Native, SearchText).
4. Cuando se muestra un anuncio se registra una **impression**.
5. Si el usuario hace clic, se registra un **click** (subconjunto de impressions).
6. Si luego realiza una acción valiosa, se registra una **conversion** (subconjunto de clicks).

El modelo completo está documentado en `00_ads_data_generator.ipynb`. El generador produce 5.000 impressions por día (1% de los 500.000 ads), 100 clicks (CTR 2%) y 8 conversions (CVR 8%).

## Objetivo

Construir un pipeline Bronze → Silver → Gold idempotente, que responda 8 preguntas de negocio sobre inversión, embudo y retorno de las campañas.

## Enfoque técnico

1. **Setup** (`01`): catálogo `dev`, schema `bronze_ads` y Volume `landing_ads`.
2. **Generación y aterrizaje** (`00` + `02`): datos sintéticos en JSON Lines, parametrizables por rango de fechas.
3. **Bronze** (`02`): `COPY INTO` incremental hacia `dev.bronze_ads.bronze_*`, sin transformar.
4. **Silver dimensiones** (`03`): dedup por `id`, normalización y nulos explícitos, en `dev.silver_ads`.
5. **Silver eventos** (`04`): `event_key`, tipos de fecha e integridad en cascada impressions → clicks → conversions.
6. **Gold** (`05`, `06`): tablas puente de atribución y una tabla por pregunta en `dev.gold_ads`, con joins, funciones de ventana y columnas derivadas.
7. **Verificación**: cada notebook cierra con una celda que contrasta sus totales contra la capa anterior.

## Decisiones técnicas relevantes

### `COPY INTO` y tablas creadas sin columnas (Bronze)
`COPY INTO` registra qué archivos ya cargó, así que re-ejecutar el mismo archivo no duplica. Las tablas se crean sin columnas para que infiera el schema del JSON en la primera carga. `mergeSchema` aparece dos veces: en `FORMAT_OPTIONS` unifica el schema entre archivos de origen y en `COPY_OPTIONS` evoluciona la tabla destino.

### Bronze sin limpieza
Duplicados, huérfanos y nulos se dejan tal cual. Así se puede auditar lo que llegó y reprocesar con otras reglas.

### `event_key` en lugar de `id` para los eventos (Silver)
El `id` se reinicia en cada archivo ingerido. `event_key = xxhash64(todos los campos)` hace que filas idénticas colapsen (dedup) y eventos distintos con el mismo `id` no se pisen.

### `MERGE` por clave en Silver
Todas las escrituras son `MERGE` por `id` (dimensiones) o `event_key` (eventos). Re-ejecutar N veces deja la tabla igual. Las tablas Silver se crean con schema explícito, para que el contrato de la capa quede escrito y un cambio inesperado en Bronze sea visible.

### Valores en cero pasan a `NULL`
Un presupuesto de $0 o un costo de impresión de $0 no son hechos reales sino datos faltantes. Se convierten a `NULL` en vez de borrar la fila o inventar un valor. Lo mismo con `creative_url` nulo: no se inventa una URL.

### Integridad en cascada contra Silver, no contra Bronze
Los clicks se validan contra las impresiones de Silver y las conversiones contra los clicks de Silver. Los 16 clicks huérfanos (`impression_id = 9999999`) se excluyen. La regla se evalúa contra los datos válidos y no contra el valor 9999999 hardcodeado.

### Atribución en tablas puente (Gold)
Como un `impression_id` puede corresponder a más de una impresión, el clic se atribuye a la impresión con ese id **más cercana en el tiempo, prefiriendo la anterior al clic** (`row_number() OVER (PARTITION BY clic ORDER BY ...) = 1`). Lo mismo de conversión a clic. La regla vive una sola vez en `clicks_atribuidos` y `conversions_atribuidas`; las preguntas solo joinean contra ellas, y por eso quedan consistentes entre sí.

### `CREATE OR REPLACE TABLE` en Gold
Con ~160.000 eventos, recalcular cada tabla agregada completa es barato y evita lógica incremental. El resultado no depende de corridas anteriores.

### Umbral anti-ruido en Q8
Solo se rankean países con al menos 500 impresiones. Sin umbral, un país con 3 impresiones y 1 conversión "ganaría" con 33%. (Con los datos actuales ningún país queda fuera.)

## Resultados principales

| Área analizada | Hallazgo | Interpretación |
|---|---|---|
| Q1 Presupuesto por industria/canal | Gaming lidera; Gaming-Social $220.408 diarios (7,27% del total) | Concentración moderada: una distribución pareja daría ~5% por combinación |
| Q2 Presupuesto usado | Ninguna campaña pasó del 0,79% de su presupuesto | Presupuestos sobredimensionados para el volumen simulado |
| Q3 CTR por canal | Search 2,118%, Video 1,959%, Social 1,948%, Display 1,941% | CTR casi uniforme entre canales |
| Q4 CPA por canal | Display $21,79 vs Search $34,07 | Search tiene el mejor CTR pero convierte peor: sus conversiones salen más caras |
| Q5 Embudo | En los 4 canales la mayor pérdida es impresión → clic (~2%) frente a clic → conversión (6-10%) | El cuello de botella es captar el clic |
| Q6 ROAS por industria | Retail 1,52; FinTech 0,82; Gaming 0,82; SaaS 0,73; Travel 0,58; global 0,89 | Solo Retail recupera más de lo invertido |
| Q7 Campañas sin conversión | 1.903 de 2.000 (95%), $6.084,46 = 92,5% del gasto | Esperable con solo 256 conversiones; el gasto sin retorno está repartido parejo entre los 4 canales |
| Q8 Países | Top por tasa: HN 0,99%, LS 0,87%, TL 0,87%; CPA más barato GR $3,68 | Tasas casi parejas: el generador asigna el país al azar |

## Estructura del proyecto

```
utn-databricks/
├── README.md
├── .gitignore
├── 00_ads_data_generator.ipynb     # generador de datos sintéticos (funciones get_*) y documentación del modelo
├── 01_create_catalog_schema.ipynb  # catálogo dev, schema bronze_ads y Volume landing_ads
├── 02_ingesta_bronze_ads.ipynb     # aterriza JSONL en el Volume y carga Bronze con COPY INTO
├── 03_silver_dimensiones.ipynb     # silver de advertisers, campaigns y ads (dedup, normalización, NULLs)
├── 04_silver_eventos.ipynb         # silver de impressions, clicks y conversions (event_key, tipos, integridad)
├── 05_gold_preguntas_1_5.ipynb     # tablas puente de atribución + Q1 a Q5
└── 06_gold_preguntas_6_8.ipynb     # Q6 (ROAS), Q7 (campañas sin conversión), Q8 (países)
```

## Cómo reproducir

**Requisitos**
- Workspace de Databricks con Unity Catalog y permiso para crear catálogos, schemas y Volumes.
- Librería `faker` (si el cluster no la trae: `%pip install faker`).

**Pasos**
1. Importá los 7 notebooks al mismo directorio del workspace.
2. Ejecutá `01_create_catalog_schema` una sola vez.
3. En `02_ingesta_bronze_ads`, ajustá el `%run` de la primera celda a la ruta de tu copia de `00_ads_data_generator` (con ambos en la misma carpeta alcanza `%run ./00_ads_data_generator`).
4. Corré `02_ingesta_bronze_ads` por cada rango de fechas que quieras cargar (widgets `start_date` y `end_date`, formato `YYYY-MM-DD`; vacíos = ayer UTC). Los números de este README salen de un backfill de 30 días de octubre 2026 más 2 corridas de un día en noviembre.
5. Ejecutá en orden `03`, `04`, `05` y `06`. Cada uno termina con una celda de verificación.

> Los nombres de catálogo y schema (`dev`, `bronze_ads`, `silver_ads`, `gold_ads`) están escritos en el SQL de los notebooks; si usás otros, hay que reemplazarlos. Los gráficos de Gold son visualizaciones de Databricks y no se renderizan fuera del workspace.

## Limitaciones y próximos pasos

**Limitaciones**
- **Datos sintéticos y aleatorios**: país, industria y canal se asignan al azar, así que las diferencias entre ellos (ROAS por industria, tasas por país) probablemente sean ruido. No se probó significancia estadística y con 256 conversiones las muestras son chicas.
- **Atribución heurística**: "impresión más cercana, preferentemente anterior" resuelve la ambigüedad de ids repetidos pero no garantiza que sea la impresión real.
- **Caso límite aceptado**: 1 conversión apunta a `click_id = 24`, que es a la vez un clic huérfano y dos clics válidos; se conserva porque existe un camino válido.
- **Dimensiones recargadas en Bronze**: cada archivo nuevo reaterriza las 500.000 filas de ads. Silver lo absorbe con dedup, pero Bronze crece sin necesidad.
- **IDs de eventos reiniciados por archivo**: obliga a usar `event_key` y a una atribución por tiempo.
- **Sin orquestación ni tests automáticos**: los notebooks se corren a mano en orden.
- **Comentario desactualizado** en el generador: `IMPRESSION_RATE = 0.01` tiene un comentario que dice 0,05%.

**Próximos pasos**
1. Aterrizar las dimensiones una sola vez (o con nombre de archivo fijo) para evitar recargas en Bronze.
2. Orquestar `02` a `06` con Databricks Workflows y carga diaria programada.
3. Agregar tests de calidad de datos (por ejemplo, expectativas sobre unicidad de `event_key` e integridad referencial).
