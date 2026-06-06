# AGENTS.md — guide for AI agents

This repo **is a Portolan spatial-data catalog** of the Spanish **Catastro** (cadastre): one publisher,
git-tracked definition, served as static files on public object storage. No server, no keys.

**Endpoint:** `https://storage.googleapis.com/catastro-es-portolan` · CRS **EPSG:25830** ·
data **Catastro INSPIRE** (non-protected). Read `portolan.config.json`.

## Read (no credentials)
- **ATTACH:** `ATTACH 'cat' (TYPE iceberg, ENDPOINT '<public_base>', AUTHORIZATION_TYPE 'none');` → `cat.v3.<id>`
- **iceberg_scan:** `iceberg_scan('<public_base>/data/v3/<id>/metadata/v1.metadata.json')`
- **remote GeoParquet:** `read_parquet('<public_base>/data/parquet/<id>.parquet')`
- **discover:** `catalog.datasets` (stac-geoparquet index).

Datasets: `edificios`, `parcelas`, `direcciones`. Geometry `geom` (EPSG:25830) + `bbox` struct; every feature
has `cod_municipio` + `provincia`. `edificios` carries año de construcción (`beginning`), `currentUse`,
`value` (superficie m²), `numberOfDwellings`, plantas, `reference` (refcat).

## Scope
Fase 1 = Comunidad de Madrid (179 municipios). Fase 2 = resto de España por provincias (mismo pipeline).
País Vasco y Navarra: catastro foral (fuera de este servicio INSPIRE).

## Build / contribute
The catalog is rebuilt by `tools/` (see README): `cat_bulk.py` (download+convert per municipio/theme; needs
`-k` for the FNMT cert and `%20` URL-encoding; **foreground only** — background loses Catastro egress) →
`assemble_catastro.py` → `build_catalog.py` (testbed venv) → `upload.py`. Git holds the definition; the bucket
holds data + generated artifacts. Never commit parquet/GML.

## Out of scope
Protected data (owner / cadastral value) — authenticated per-parcel only, never bulk. This catalog is
**open, public, anonymous**.
