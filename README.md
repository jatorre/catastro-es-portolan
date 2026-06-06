# Catastro · Portolan 🟫

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
