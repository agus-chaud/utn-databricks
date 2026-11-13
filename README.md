# Pipeline de datos de publicidad digital en Databricks (capa Bronze)

Ingesta incremental de datos sintéticos de AdTech (6 entidades, 1 día de eventos por corrida) a tablas Delta en Unity Catalog, usando Volumes + `COPY INTO`. Proyecto de formación como data engineer en Databricks (TP integrador, UTN).

> Estado: **Bronze completo**. Silver y Gold están planificadas (ver [Limitaciones y próximos pasos](#limitaciones-y-próximos-pasos)).

## Hallazgos clave

- **`COPY INTO` es idempotente por archivo**: re-ejecutar el notebook de ingesta sobre los mismos archivos no duplica filas; la verificación final coincide con lo aterrizado (200 / 2.000 / 500.000 / 5.023 / 100 / 8 filas en la corrida del 2026-11-08).
- **Bronze conserva todos los defectos de calidad del origen, sin corregirlos**: 5.023 impressions para 5.000 generadas (**~0,5%** duplicadas), **1%** de ads sin `creative_url`, **1%** de campañas con presupuesto 0, **2%** de websites sin protocolo.
- **Hay claves huérfanas por diseño**: **0,5%** de clicks apuntan a `impression_id = 9999999` y **1%** de conversions a `click_id = 8888888`, que no existen en sus tablas padre.
- **Las dimensiones se reescriben completas en cada corrida** (500.000 ads por archivo). Con un archivo nuevo por fecha, `COPY INTO` lo trata como archivo nuevo y vuelve a cargar los 500.000 ads (detalle en Limitaciones).

## Resumen ejecutivo

Trabajo de práctica para consolidar el flujo de ingesta en Databricks: un notebook genera datos crudos del dominio publicidad digital y otro los aterriza como JSON Lines en un Volume y los carga a Bronze. Ambos son parametrizables por rango de fechas, así que sirven tanto para la carga diaria como para un backfill.

## Problema

Los datos de origen llegan con defectos (duplicados, claves huérfanas, nulos, valores en cero). Si la capa Bronze los "arregla" al ingerir, se pierde la trazabilidad de qué llegó realmente y no se puede reprocesar con otras reglas. El desafío de esta etapa es **ingerir sin transformar** y dejar los defectos documentados para que Silver los trate con reglas explícitas.

## Dominio de negocio

Embudo clásico de publicidad digital:

```
Advertiser → Campaign → Ad → Impression → Click → Conversion
```

1. Un **advertiser** (empresa) quiere promocionar un producto.
2. Crea **campaigns** con objetivo, canal (Search, Social, Display, Video) y presupuesto diario.
3. Cada campaña tiene **ads** (creatividades: Banner, Video, Native, SearchText).
4. Cuando se muestra un anuncio a un usuario se registra una **impression**.
5. Si el usuario hace clic, se registra un **click** (subconjunto de impressions).
6. Si luego realiza una acción valiosa (compra, registro), se registra una **conversion** (subconjunto de clicks).

Volúmenes por día en el generador: 5.000 impressions (1% de los ads), 100 clicks (CTR 2%) y 8 conversions (CVR 8%).

## Objetivo

Ingerir las 6 entidades del dominio en tablas Delta Bronze de forma incremental, parametrizable por fecha y sin duplicar datos al re-ejecutar.

## Enfoque técnico

1. **Generación** (`00_ads_data_generator`): funciones `get_*` deterministas (seed fija) que devuelven `list[dict]` con defectos inyectados a propósito.
2. **Parametrización** (`02_ingesta_bronze_ads`): widgets `base_path`, `start_date`, `end_date`. Ambas fechas vacías = ayer (UTC); una sola = ese día; las dos = rango inclusivo.
3. **Aterrizaje**: cada entidad se escribe como `.jsonl` en `/Volumes/dev/bronze_ads/landing_ads/<entidad>/<entidad>_<fecha>.jsonl`.
4. **Ingesta**: `CREATE TABLE IF NOT EXISTS` sin columnas + `COPY INTO ... FILEFORMAT = JSON` hacia `dev.bronze_ads.bronze_<entidad>`.
5. **Verificación**: `UNION ALL` de `COUNT(*)` por tabla, para comparar contra los registros aterrizados.

## Decisiones técnicas relevantes

### JSON Lines como formato de aterrizaje
Un registro por línea, mismo formato para las 6 entidades. `COPY INTO` lo lee sin configuración extra, y se puede escribir en streaming sin cargar todo en memoria.

### `COPY INTO` en lugar de un `INSERT`/`overwrite`
`COPY INTO` registra qué archivos ya cargó, por lo que re-ejecutar no duplica. Un `INSERT` o un `append` desde Python tendría que reimplementar ese control.

### Tablas creadas sin columnas
`CREATE TABLE IF NOT EXISTS` sin schema deja que `COPY INTO` infiera las columnas del JSON en la primera carga. Evita mantener DDL a mano en una capa cuyo contrato es "lo que llegó".

### `mergeSchema` dos veces
Cumplen funciones distintas:
- `FORMAT_OPTIONS ('mergeSchema' = 'true')`: unifica el schema entre **archivos de origen** (un archivo nuevo con una columna extra no rompe la lectura).
- `COPY_OPTIONS ('mergeSchema' = 'true')`: evoluciona el schema de la **tabla destino** (agrega la columna en vez de fallar).

### Fechas validadas al inicio
`date.fromisoformat` y el chequeo `start_date > end_date` hacen que un error de parámetro falle en la primera celda con un mensaje claro, y no a mitad de la generación.

### Bronze sin limpieza
Duplicados, huérfanos y nulos se dejan tal cual. La corrección es responsabilidad de Silver, donde las reglas quedan explícitas y reprocesables.

## Resultados principales

| Área analizada | Hallazgo | Interpretación |
|---|---|---|
| Idempotencia de la ingesta | Conteos finales = registros aterrizados | `COPY INTO` evita duplicar al re-ejecutar los mismos archivos |
| `bronze_impressions` | 5.023 filas vs 5.000 generadas | ~0,5% son copias exactas; Silver debe deduplicar por `id` |
| `bronze_clicks` | 0,5% con `impression_id = 9999999` | Huérfanos: no tienen impression padre |
| `bronze_conversions` | 1% con `click_id = 8888888` | Huérfanos: no tienen click padre |
| `bronze_campaigns` | 1% con `daily_budget_usd = 0` | Valor sospechoso, no necesariamente error |
| `bronze_ads` | 1% con `creative_url` nulo | Creatividades incompletas |
| `bronze_advertisers` | 2% de websites sin `https://` | Requiere normalización |

## Estructura del proyecto

```
utn-databricks/
├── README.md                       # este archivo
├── .gitignore
├── 00_ads_data_generator.ipynb     # genera los datos sintéticos (funciones get_*) y documenta el modelo
├── 01_create_catalog_schema.ipynb  # crea el catálogo dev, el schema bronze_ads y el Volume landing_ads
└── 02_ingesta_bronze_ads.ipynb     # aterriza JSONL en el Volume y carga Bronze con COPY INTO
```

## Cómo reproducir

**Requisitos**
- Workspace de Databricks con Unity Catalog.
- Permiso para crear catálogos, schemas y Volumes en Unity Catalog (lo hace `01_create_catalog_schema`).
- Librería `faker` (si el cluster no la trae: `%pip install faker`).

**Pasos**
1. Importá los tres notebooks al mismo directorio del workspace.
2. Ejecutá `01_create_catalog_schema` una sola vez: crea `dev`, `dev.bronze_ads` y el Volume `landing_ads`.
3. En `02_ingesta_bronze_ads`, ajustá la primera celda `%run` para que apunte a tu copia de `00_ads_data_generator` (con ambos en la misma carpeta alcanza `%run ./00_ads_data_generator`).
4. Ejecutá la celda de parámetros. Sin fechas genera ayer (UTC). Para un backfill completá `start_date` y `end_date` (`YYYY-MM-DD`).
5. Ejecutá la celda de `COPY INTO`.
6. Ejecutá la celda de verificación y comparala con los conteos impresos en el paso 4.

> Las rutas de los Volumes distinguen mayúsculas/minúsculas: respetá `dev.bronze_ads` y `landing_ads` tal cual.

## Limitaciones y próximos pasos

**Limitaciones**
- **Dimensiones duplicadas entre días**: `get_advertisers/campaigns/ads` no dependen de la fecha, pero el nombre del archivo sí. Una segunda corrida con otra fecha crea otro archivo y `COPY INTO` carga de nuevo los 500.000 ads. Pendiente de verificar en una segunda corrida; la corrección sería escribir las dimensiones una sola vez o resolverlo en Silver.
- **IDs de eventos reiniciados**: `id`, `impression_id` y `click_id` se generan desde 1 en cada corrida, así que dos días distintos colisionan en los ids.
- **Datos sintéticos**: no hay conclusiones de negocio sobre campañas reales.
- **Sin pruebas automáticas** ni orquestación (los notebooks se corren a mano).
- **Comentario desactualizado**: en el generador, `IMPRESSION_RATE = 0.01` tiene un comentario que dice 0,05%.

**Próximos pasos**
1. Capa **Silver**: deduplicar impressions, tratar huérfanos, normalizar websites, decidir qué hacer con presupuestos en 0 y creatividades nulas.
2. Capa **Gold**: métricas del embudo (CTR, CVR, costo por conversión) por campaña y canal.
3. Orquestación con Databricks Workflows y carga diaria programada.
