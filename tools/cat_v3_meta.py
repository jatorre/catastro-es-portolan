#!/usr/bin/env python3
"""Escribe el metadata Iceberg v3 sobre los data files ya subidos (lee el ledger) y lo sube.
Uso: cat_v3_meta.py TEMA"""
import os, sys, json, subprocess
from pathlib import Path
sys.path.insert(0, "/tmp"); sys.path.insert(0, "/Users/jatorre/workspace/iceberg-geo-testbed")
from cat_v3_build import theme_columns, WORK, BUCKET, GS
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField
from testbed._static_catalog import write_static_catalog

theme=sys.argv[1]; cols=theme_columns(theme)
ledger=WORK/f"{theme}_datafiles.jsonl"
rows=[json.loads(l) for l in open(ledger)]
rows.sort(key=lambda d:d["NN"])
# Solo bounds/counts en id (fid 1) y geom (último fid) — como el fixture v3 probado.
# Bounds en columnas bbox hacen tropezar al pruner de Snowflake ("cast variant to REAL").
# Snowflake exige bounds en TODAS las columnas del esquema Iceberg (si falta una -> error 300010).
# El esquema oculta bbox; keep = los field-ids de las columnas NO-bbox (atributos + geom).
BBOX={"xmin","ymin","xmax","ymax"}
keep={c["fid"] for c in cols if c["name"] not in BBOX}
data_files=[]
for d in rows:
    data_files.append(dict(path=d["path"], size=d["size"], rows=d["rows"],
        lower={int(k):bytes.fromhex(v) for k,v in d["lower"].items() if int(k) in keep},
        upper={int(k):bytes.fromhex(v) for k,v in d["upper"].items() if int(k) in keep},
        value_counts={int(k):v for k,v in d["value_counts"].items() if int(k) in keep},
        null_value_counts={int(k):v for k,v in d["null_value_counts"].items() if int(k) in keep}))
# El esquema Iceberg OCULTA las columnas bbox (xmin/ymin/xmax/ymax): siguen en el parquet
# (para la poda de DuckDB vía read_parquet) pero NO se declaran a Iceberg, porque columnas
# DOUBLE extra hacen abortar la poda espacial de Snowflake (error 300010). Así Snowflake ve
# solo atributos+geom y poda por geom NATIVO; DuckDB ve todo el parquet y poda por bbox.
BBOX={"xmin","ymin","xmax","ymax"}
cols_ice=[c for c in cols if c["name"] not in BBOX]
ice=Schema(*[NestedField(c["fid"], c["name"], c["itype"], required=False) for c in cols_ice])
jfields=[{"id":c["fid"],"name":c["name"],"required":False,"type":c["jtype"],"doc":c["doc"]} for c in cols_ice]
nmap=[{"field-id":c["fid"],"names":[c["name"]]} for c in cols_ice]
loc=os.environ.get("CAT_LOC_URI", f"gs://{BUCKET}/v3/{theme}")
dest=os.environ.get("CAT_META_DEST", f"{GS}/v3/{theme}/")
mdir=os.environ.get("CAT_META_DIR", "metadata")
root=WORK/theme
mp=write_static_catalog(table_root=root, iceberg_schema=ice, schema_json_fields=jfields,
    name_mapping=nmap, data_files=data_files, format_version_in_metadata=3,
    location_uri=loc, meta_dir_name=mdir)
print("metadata local:", mp, "| location_uri:", loc)
# sube metadata/ al destino
r=subprocess.run(["gcloud","storage","cp","-r",str(root/mdir),dest,"-q"],
                 capture_output=True, text=True)
print("subido:", r.returncode, r.stderr[-200:] if r.returncode else "OK")
subprocess.run(["gcloud","storage","ls",f"{GS}/v3/{theme}/metadata/"],check=False)
print(f"== metadata v3 de {theme}: {len(data_files)} data files ==")
