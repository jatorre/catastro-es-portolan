# Catastro · Portolan 🟫

🌐 **[Explorar el catálogo (visor web) →](https://storage.googleapis.com/catastro-es-portolan/web/index.html)**

El **Catastro español** completo como catálogo cloud-native **Portolan**: edificios, parcelas y
direcciones de **toda España** re-expuestos como **GeoParquet remoto + STAC + Apache Iceberg** sobre
almacenamiento público — `read_parquet` en sitio, `ATTACH` desde DuckDB / Snowflake, sin servidor, sin
credenciales. CRS **EPSG:4326** (WGS84), particionado por provincia.

Una capa base reutilizable a escala nacional: antigüedad/uso/superficie de edificios, parcelas, y un
**gazetteer de direcciones** (base soberana de geocoding). Construido desde **Catastro INSPIRE** (datos
abiertos no protegidos: sin titularidad ni valor catastral).

## Cobertura — España completa (52 gerencias territoriales)
| id | qué | features | atributos clave |
|---|---|---:|---|
| `edificios` | huellas de edificios | **12.489.197** | **año de construcción, uso, superficie m², nº viviendas, plantas**, refcat |
| `parcelas` | parcelas catastrales | **51.953.175** | refcat, superficie |
| `direcciones` | portales (puntos) | **15.886.629** | calle, número, CP — *gazetteer* |

**~80,3 M de features.** Cada feature lleva `cod_municipio` + `provincia`. Esquema canónico:
`geom` GEOMETRY(EPSG:4326) + `bbox` STRUCT, ordenado Hilbert, ZSTD, GeoParquet 1.1 validado.

Cubre las **52 gerencias territoriales** del Catastro INSPIRE (territorio común, península + Baleares +
Canarias + Ceuta + Melilla). **Quedan fuera** País Vasco (Álava `01`, Gipuzkoa `20`, Bizkaia `48`) y
Navarra (`31`): tienen **catastro foral propio**, no servido por este servicio estatal.

> **Gerencias ≠ provincias INE.** El Catastro usa códigos de *gerencia territorial*, casi iguales a los
> de provincia INE salvo que **4 provincias se parten en dos** y las ciudades autónomas tienen código
> propio: `51`=Cartagena (2ª de Murcia), `53`=Jerez de la Frontera (2ª de Cádiz), `54`=Vigo (2ª de
> Pontevedra), `55`=Ceuta, `56`=Melilla.

## Endpoint
```
https://storage.googleapis.com/catastro-es-portolan
```

### Acceso nacional — GeoParquet particionado (vía canónica, sin credenciales)
Lectura anónima vía interoperabilidad S3 de GCS (el bucket es público; el glob `*.parquet` funciona
porque `allUsers:objectViewer` permite listar):
```sql
INSTALL httpfs;LOAD httpfs;INSTALL spatial;LOAD spatial;
CREATE SECRET g (TYPE s3, PROVIDER config, KEY_ID '', SECRET '',
  ENDPOINT 'storage.googleapis.com', URL_STYLE 'path', USE_SSL true, REGION 'auto');

-- toda España, podando por partición de provincia (Hive)
SELECT count(*) FROM read_parquet(
  's3://catastro-es-portolan/data/parquet/edificios/*.parquet', hive_partitioning=1)
WHERE provincia='28';                                  -- edificios de Madrid

-- edificios por década de construcción en una provincia
SELECT left(beginning,4) AS anio, count(*) FROM read_parquet(
  's3://catastro-es-portolan/data/parquet/edificios/provincia=08.parquet')
GROUP BY 1 ORDER BY 1;
```

> Una sola provincia: lee el fichero directo `provincia=NN.parquet`. Varias/España entera: usa el glob
> `*.parquet` con `hive_partitioning=1` y **filtra siempre por `provincia`** (o por `bbox`) para podar.

### Iceberg / STAC (ATTACH)
```sql
INSTALL iceberg;LOAD iceberg;INSTALL httpfs;LOAD httpfs;INSTALL spatial;LOAD spatial;
ATTACH 'cat' (TYPE iceberg, ENDPOINT 'https://storage.googleapis.com/catastro-es-portolan',
              AUTHORIZATION_TYPE 'none');
SHOW ALL TABLES;                                       -- v3.edificios/parcelas/direcciones, catalog.datasets
```
> El índice Iceberg/STAC se generó en la fase 1 (Comunidad de Madrid, fichero único). La fuente de verdad
> nacional es el GeoParquet particionado de arriba; regenerar el índice Iceberg sobre el particionado es
> el siguiente paso (ver `tools/build_catalog.py`).

## Formato v3 unificado (geom nativo + bbox) — un juego de ficheros, dos motores
Bajo `…/v3/{tema}/` está el **mismo dato re-expuesto en un formato que sirve a la vez a DuckDB y a
Snowflake/Iceberg**, sin duplicar almacenamiento. Los ficheros de datos de la tabla Iceberg v3 **son
GeoParquet normal**: puedes leerlos directos con `read_parquet` o registrarlos como tabla Iceberg.

```
gs://catastro-es-portolan/v3/{edificios,parcelas,direcciones}/
  data/provincia={NN}.parquet      ← GeoParquet 2.0 (un fichero por gerencia)
  metadata/v1.metadata.json + *.avro  ← Iceberg v3 (manifest con bounds de geom packed_xy_le)
```

**Cada motor usa su vía de poda NATIVA** sobre el mismo fichero:
- **`geom`** → tipo lógico **`Geometry(crs=srid:4326)` nativo de Parquet** (GeoParquet 2.0). Snowflake lo
  materializa como `GEOMETRY(4326)` y **poda por `ST_Intersects(geom,…)` nativo, sin bbox**. DuckDB lo lee
  como `GEOMETRY` sin cast.
- **`xmin, ymin, xmax, ymax`** (DOUBLE) → bounding box por fila. **Para DuckDB** (poda por predicado
  numérico). Están en el parquet pero **OCULTAS del esquema Iceberg**, así Snowflake no las ve y poda por geom.
- Atributos del Catastro con **nombre limpio + descripción embebida** (`field_id` + `description`).

> **Tres requisitos para que Snowflake pode el geom externo** (descubiertos midiendo; ver
> [duckdb-iceberg#1002](https://github.com/duckdb/duckdb-iceberg/issues/1002) para el lado DuckDB):
> **(1)** CRS del tipo Parquet = exactamente `srid:4326` (vacío → `cast variant to REAL`);
> **(2)** `lower/upper` bounds en el manifest para **todas** las columnas del esquema (placeholder en
> columnas all-null como `floors_above`); **(3)** field-id de `geom` **contiguo, sin huecos** (por eso
> `geom` va antes que `bbox` en el parquet). Si falta cualquiera → error interno `300010`.

### Snowflake — poda NATIVA por geom (sin bbox)
El external volume debe estar en la **misma región** que la cuenta Snowflake; los datos públicos están en
`europe-southwest1` (si tu cuenta está en otra región, espeja el `v3/` a un bucket de tu región).
```sql
CREATE OR REPLACE ICEBERG TABLE catastro_edificios
  EXTERNAL_VOLUME='<vol_misma_region>' CATALOG='<object_store_catalog>'
  METADATA_FILE_PATH='cat-edif-v3/metadata/v1.metadata.json';

-- consulta espacial nativa: Snowflake poda micro-particiones por los bounds de geom (sin bbox)
SELECT COUNT(*) FROM catastro_edificios
WHERE ST_INTERSECTS(geom, ST_GEOMFROMWKT('POLYGON((-3.71 40.41,-3.69 40.41,-3.69 40.43,-3.71 40.43,-3.71 40.41))',4326));
-- medido: 1/52 micro-particiones escaneadas para una caja de ~2 km sobre 12,5 M de edificios.
```

### DuckDB — lectura directa + poda por bbox
DuckDB **no** dispara poda desde `ST_Intersects(geom,…)` todavía
([duckdb-iceberg#1002](https://github.com/duckdb/duckdb-iceberg/issues/1002)); por eso en DuckDB se usa el
predicado **`bbox`** (poda por stats de row-group), leyendo el parquet directo:
```sql
INSTALL httpfs;LOAD httpfs;INSTALL spatial;LOAD spatial;
CREATE SECRET g (TYPE s3, PROVIDER config, KEY_ID '', SECRET '',
  ENDPOINT 'storage.googleapis.com', URL_STYLE 'path', USE_SSL true, REGION 'auto');

SELECT count(*) FROM read_parquet(
  's3://catastro-es-portolan/v3/edificios/data/*.parquet', hive_partitioning=1)
WHERE provincia='28' AND xmin BETWEEN -3.71 AND -3.69 AND ymin BETWEEN 40.41 AND 40.43;

-- geometría exacta tras el prefiltro bbox
SELECT reference, year_built, area_m2 FROM read_parquet(
  's3://catastro-es-portolan/v3/edificios/data/provincia=28.parquet')
WHERE xmin BETWEEN -3.71 AND -3.69 AND ymin BETWEEN 40.41 AND 40.43
  AND ST_Intersects(geom, ST_MakeEnvelope(-3.71,40.41,-3.69,40.43));
```

### Matriz de soporte (medido)
| operación | DuckDB (`read_parquet`) | Snowflake (Iceberg externo) |
|---|:--:|:--:|
| Leer `geom` como geometría nativa | ✅ `GEOMETRY` | ✅ `GEOMETRY(4326)` |
| Leer atributos + descripciones | ✅ | ✅ (no ve bbox: oculto del esquema) |
| **Poda espacial nativa** | por **`bbox`** (stats row-group) | por **`ST_Intersects(geom)`** — **1/52** part. |
| `ST_Intersects(geom)` poda | ❌ todavía (usa bbox) | ✅ nativo |

**Conclusión:** un único juego de ficheros; **Snowflake consulta por `geom` (poda nativa, sin bbox)** y
**DuckDB por `bbox`**. Las columnas bbox viven en el parquet (para DuckDB) pero ocultas del esquema Iceberg
para no interferir con la poda de geom de Snowflake.

### Diccionario de campos
**`edificios`** — `reference` (refcat 14c) · `local_id` · `condition` (estado: functional/ruin/…) ·
`year_built` (año constr., INSPIRE *beginning*) · `current_use` (1_residential, 3_industrial, …) ·
`num_units` · `num_dwellings` (nº viviendas) · `floors_above` (plantas sobre rasante) ·
`area_m2` (superficie construida) · `cod_municipio` · `provincia` · `xmin/ymin/xmax/ymax` · `geom`.

**`parcelas`** — `reference` (refcat nacional) · `local_id` · `label` (nº parcela) ·
`area_m2` (superficie) · `cod_municipio` · `provincia` · `xmin/ymin/xmax/ymax` · `geom`.

**`direcciones`** — `local_id` · `designator` (nº de policía) · `type` · `level` · `specification`
(entrance/parcel/building) · `method` · `cod_municipio` · `provincia` · `xmin/ymin/xmax/ymax` · `geom`.

Construido por `tools/cat_v3_build.py` (re-encode geom nativo + bbox + descripciones) y
`tools/cat_v3_meta.py` (metadata Iceberg v3 con bounds en id+geom).

## Cómo se construye (notas técnicas — gotchas reales)
Fuente: **Catastro INSPIRE ATOM**, por municipio y tema (Buildings/CadastralParcels/Addresses), GML zip,
refresco ~6 meses. Pipeline por **gerencia** en `tools/` (resumable, salta lo ya hecho):
1. **Descarga+convierte por municipio.** Tres escollos resueltos:
   - El servidor usa certificado **FNMT** (no en el store de curl) → `-k`.
   - Las **URLs llevan espacios** (ej. `39103-CDAD CAM CAB/...`) → `%20`.
   - El **CRS varía por gerencia** (España abarca 3 husos UTM: 25829/25830/25831) → se **reproyecta cada
     municipio a EPSG:4326** con `ogr2ogr -t_srs EPSG:4326` para poder unir todo el país.
2. **Ensambla por gerencia:** une municipios + inyecta `cod_municipio`/`provincia` + `gpio convert`
   (bbox + Hilbert + ZSTD) → 1 GeoParquet/tema. Munis sin features (GML vacío, p.ej. registros tipo
   `CDAD CAM CAB`) se excluyen de la unión.
3. **Sube** a `gs://catastro-es-portolan/data/parquet/{tema}/provincia={NN}.parquet`.

## Datos protegidos (NO incluidos)
Titularidad (propietario) y valor catastral por parcela son datos protegidos: solo consulta autenticada
(Cl@ve / certificado) en la Sede del Catastro, nunca en bloque. Aquí solo lo **abierto** (geometría +
características físicas). Catálogo **público, anónimo, sin claves**.
