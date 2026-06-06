# Catastro · Portolan

The Spanish **Catastro** (cadastre) as a cloud-native **Portolan** catalog: buildings, parcels and
addresses re-exposed as **Apache Iceberg + remote GeoParquet + STAC** on public object storage —
`ATTACH` from DuckDB / Snowflake, `read_parquet` in place, no server, no credentials. Native **EPSG:25830**.

A reusable base layer: building age/use/surface, parcels, and an **address gazetteer** (a sovereign
geocoding base). Built from **Catastro INSPIRE** open data (non-protected: no owner, no cadastral value).

## Endpoint
```
https://storage.googleapis.com/catastro-es-portolan
```
```sql
INSTALL iceberg;LOAD iceberg;INSTALL httpfs;LOAD httpfs;INSTALL spatial;LOAD spatial;
ATTACH 'cat' (TYPE iceberg, ENDPOINT 'https://storage.googleapis.com/catastro-es-portolan',
              AUTHORIZATION_TYPE 'none');
SHOW ALL TABLES;                              -- v3.edificios, v3.parcelas, v3.direcciones, catalog.datasets
SELECT count(*) FROM cat.v3.edificios WHERE cod_municipio='28900';   -- edificios de Madrid capital
-- directo (sin ATTACH): read_parquet('…/data/parquet/edificios.parquet')
```

## Datasets (fase 1 — Comunidad de Madrid, 179 municipios)
| id | qué | nº | atributos |
|---|---|---:|---|
| `edificios` | huellas de edificios | 587.257 | **año de construcción, uso, superficie m², nº viviendas, plantas**, refcat |
| `parcelas` | parcelas catastrales | 1.109.362 | refcat, superficie |
| `direcciones` | portales (puntos) | 777.777 | calle, número, CP — *gazetteer* |

Cada feature lleva `cod_municipio` + `provincia`. Esquema canónico (igual que el resto de catálogos):
`geom` GEOMETRY(EPSG:25830) + `bbox` STRUCT, ordenado Hilbert, ZSTD, GeoParquet 1.1 validado.

## Alcance / fases
- **Fase 1 (publicada):** Comunidad de Madrid (provincia 28), 3 temas.
- **Fase 2:** resto de España **por provincias** (mismo pipeline, resumable). País Vasco y Navarra tienen
  catastro foral propio (no en este servicio INSPIRE).

## Cómo se construye (notas técnicas — gotchas reales)
Fuente: **Catastro INSPIRE ATOM**, por municipio y tema (Buildings/CadastralParcels/Addresses), GML zip,
EPSG:25830, refresco ~6 meses. Pipeline en `tools/`:
1. `cat_bulk.py BU|CP|AD` — descarga+convierte por municipio. **Dos escollos resueltos:** el servidor usa
   certificado **FNMT** (no en el store de curl → `-k`/CA FNMT) y las **URLs llevan espacios** (`%20`).
   *Las descargas solo funcionan en ejecución **foreground** (un proceso en background pierde la red a Catastro).*
2. `assemble_catastro.py` — une los municipios + inyecta `cod_municipio` + `gpio convert` (bbox+Hilbert+ZSTD) → 1 GeoParquet/tema.
3. `build_catalog.py` (venv iceberg-geo-testbed) — v3 Iceberg + índice stac-geoparquet + superficie REST.
4. `upload.py` — publica a `gs://catastro-es-portolan` (rsync `data/` + superficie `v1/`).

## Datos protegidos (NO incluidos)
Titularidad (propietario) y valor catastral por parcela son datos protegidos: solo consulta autenticada
(Cl@ve/certificado) en la Sede del Catastro, nunca en bloque. Aquí solo lo abierto (geometría + características).
