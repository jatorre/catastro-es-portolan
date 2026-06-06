# AGENTS.md — guide for AI agents

This repo **is a Portolan spatial-data catalog** of the Spanish **Catastro** (cadastre): one publisher,
git-tracked definition, served as static files on public object storage. No server, no keys.

**Endpoint:** `https://storage.googleapis.com/catastro-es-portolan` · CRS **EPSG:4326** ·
data **Catastro INSPIRE** (non-protected) · scope **all of Spain** (52 territorial offices). Read
`portolan.config.json`.

## Read (no credentials)
- **National GeoParquet (canonical) — Hive-partitioned by province:**
  ```sql
  CREATE SECRET g (TYPE s3, PROVIDER config, KEY_ID '', SECRET '',
    ENDPOINT 'storage.googleapis.com', URL_STYLE 'path', USE_SSL true, REGION 'auto');
  read_parquet('s3://catastro-es-portolan/data/parquet/edificios/*.parquet', hive_partitioning=1)
  ```
  Always filter by `provincia='NN'` (or by `bbox`) to prune. One province → read `provincia=NN.parquet`
  directly. (HTTP globs are NOT supported; the S3-interop glob works because `allUsers:objectViewer`
  grants list.)
- **ATTACH (Iceberg/STAC):** `ATTACH 'cat' (TYPE iceberg, ENDPOINT '<public_base>', AUTHORIZATION_TYPE 'none');`
  → `cat.v3.<id>`, `cat.catalog.datasets`. *Note:* the Iceberg/STAC index was built in phase 1 (Comunidad
  de Madrid, single-file, EPSG:25830); the national source of truth is the partitioned GeoParquet above.

Datasets: `edificios` (12.5M), `parcelas` (52.0M), `direcciones` (15.9M). Geometry `geom` (EPSG:4326) +
`bbox` struct; every feature has `cod_municipio` + `provincia`. `edificios` carries año de construcción
(`beginning`), `currentUse`, `value` (superficie m²), `numberOfDwellings`, plantas, `reference` (refcat).

## Scope — 52 territorial offices (gerencias), not INE provinces
Codes mostly match INE province codes **except** 4 provinces split in two + the autonomous cities have
own codes: `51`=Cartagena (2nd of Murcia), `53`=Jerez (2nd of Cádiz), `54`=Vigo (2nd of Pontevedra),
`55`=Ceuta, `56`=Melilla. **Excluded:** País Vasco (`01`/`20`/`48`) and Navarra (`31`) — *catastro foral*,
not in this INSPIRE service. The authoritative list is the master ATOM
(`INSPIRE/Buildings/ES.SDGC.BU.atom.xml` → `atom_NN.xml` codes).

## Build / contribute (gotchas — all real)
Source: **Catastro INSPIRE ATOM**, per municipio/theme, GML zip. Pipeline in `tools/`, **resumable**:
1. Download+convert per municipio. **(a)** server uses the **FNMT** cert → `curl -k`; **(b)** URLs contain
   **spaces** → `%20`; **(c)** CRS varies per office (UTM zones 25829/25830/25831) → **reproject every
   municipio to EPSG:4326** (`ogr2ogr -t_srs EPSG:4326`) so the whole country can be unioned.
2. Assemble per office: union munis + inject `cod_municipio`/`provincia` + `gpio convert` (bbox+Hilbert+ZSTD).
   **Skip empty munis** (GML with no features, e.g. `CDAD CAM CAB`-type records) — one empty/0-byte parquet
   breaks the `union_by_name`.
3. Upload to `gs://catastro-es-portolan/data/parquet/{tema}/provincia={NN}.parquet`.

Geometry column from GML/gpio is `geometry`/`geometry_bbox`; the canonical schema needs `geom`/`bbox` —
rename via DuckDB `COPY (SELECT * RENAME (geometry AS geom, geometry_bbox AS bbox) ...)` (preserves the
GEOMETRY type + GeoParquet metadata). Git holds the definition; the bucket holds data. Never commit parquet/GML.

## Out of scope
Protected data (owner / cadastral value) — authenticated per-parcel only, never bulk. This catalog is
**open, public, anonymous**.
