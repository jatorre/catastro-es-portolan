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
geom_fid=max(c["fid"] for c in cols); keep={1, geom_fid}
nob=os.environ.get("CAT_NO_BOUNDS")=="1"
data_files=[]
for d in rows:
    df=dict(path=d["path"], size=d["size"], rows=d["rows"])
    if not nob:
        df.update(
            lower={int(k):bytes.fromhex(v) for k,v in d["lower"].items() if int(k) in keep},
            upper={int(k):bytes.fromhex(v) for k,v in d["upper"].items() if int(k) in keep},
            value_counts={int(k):v for k,v in d["value_counts"].items() if int(k) in keep},
            null_value_counts={int(k):v for k,v in d["null_value_counts"].items() if int(k) in keep})
    else:
        df.update(lower={}, upper={}, value_counts={int(k):v for k,v in d["value_counts"].items() if int(k) in keep},
                  null_value_counts={int(k):v for k,v in d["null_value_counts"].items() if int(k) in keep})
    data_files.append(df)
ice=Schema(*[NestedField(c["fid"], c["name"], c["itype"], required=False) for c in cols])
jfields=[{"id":c["fid"],"name":c["name"],"required":False,"type":c["jtype"],"doc":c["doc"]} for c in cols]
nmap=[{"field-id":c["fid"],"names":[c["name"]]} for c in cols]
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
