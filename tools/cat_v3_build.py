#!/usr/bin/env python3
"""Catastro -> Iceberg V3 unificado (geom nativo GeoParquet 2.0 + bbox plano + descripciones).
Un solo juego de ficheros sirve a DuckDB (read_parquet/bbox) y Snowflake/Iceberg (geom nativo).

Por tema y provincia: lee el GeoParquet 1.1 actual (s3 anon), re-encoda geom al tipo lógico
Geometry nativo (geoarrow), aplana bbox a 4 DOUBLE, adjunta field_id + descripción a cada columna,
sube a gs://catastro-es-portolan/v3/{tema}/data/provincia=NN.parquet, y registra el data_file
(con bounds packed_xy_le en geom, LE-double en bbox, UTF8 en id) en datafiles.jsonl. Resumable.
Cuando estan las 52 -> write_static_catalog (metadata v3) + sube metadata/.

Uso: cat_v3_build.py TEMA [NN ...]
"""
import os, sys, json, struct, subprocess, hashlib
from pathlib import Path
import pyarrow as pa, pyarrow.parquet as pq, pyarrow.compute as pc
import geoarrow.pyarrow as ga
from pyiceberg.schema import Schema
from pyiceberg.types import (NestedField, StringType, IntegerType, DoubleType, BinaryType)
sys.path.insert(0, "/Users/jatorre/workspace/iceberg-geo-testbed")
from testbed._static_catalog import write_static_catalog

BUCKET = "catastro-es-portolan"
PUBLIC = f"https://storage.googleapis.com/{BUCKET}"
GS = f"gs://{BUCKET}"
SRC = f"s3://{BUCKET}/data/parquet"          # GeoParquet 1.1 actual (anon S3-interop)
WORK = Path("/tmp/v3work"); WORK.mkdir(exist_ok=True)
GEOM_EXT = ga.wkb().with_crs("srid:4326")   # Snowflake exige crs=srid:4326 en el tipo lógico Parquet
DUCK_SECRET = ("INSTALL httpfs;LOAD httpfs;INSTALL spatial;LOAD spatial;"
    "CREATE SECRET g (TYPE s3, PROVIDER config, KEY_ID '', SECRET '', "
    "ENDPOINT 'storage.googleapis.com', URL_STYLE 'path', USE_SSL true, REGION 'auto');")

# (src_col, out_name, arrow_type, iceberg_type, json_type, descripcion)
COMMON_TAIL = [
    ("cod_municipio","cod_municipio", pa.string(), StringType(), "string", "Código de municipio DGC (2 díg. gerencia + 3 municipio)."),
    ("provincia","provincia", pa.string(), StringType(), "string", "Código de gerencia territorial DGC (≈ provincia INE; 51 Cartagena, 53 Jerez, 54 Vigo, 55 Ceuta, 56 Melilla)."),
]
BBOX_COLS = [
    ("xmin", pa.float64(), DoubleType(), "double", "Longitud mínima (grados, EPSG:4326) del bounding box de la geometría. Prefiltro para poda en DuckDB."),
    ("ymin", pa.float64(), DoubleType(), "double", "Latitud mínima (grados, EPSG:4326) del bounding box."),
    ("xmax", pa.float64(), DoubleType(), "double", "Longitud máxima (grados, EPSG:4326) del bounding box."),
    ("ymax", pa.float64(), DoubleType(), "double", "Latitud máxima (grados, EPSG:4326) del bounding box."),
]
THEMES = {
 "edificios": [
    ("reference","reference", pa.string(), StringType(), "string", "Referencia catastral del edificio (refcat, 14 car.)."),
    ("localId","local_id", pa.string(), StringType(), "string", "Identificador local INSPIRE del edificio."),
    ("conditionOfConstruction","condition", pa.string(), StringType(), "string", "Estado de la construcción (functional, declined, ruin, demolished...)."),
    ("beginning","year_built", pa.string(), StringType(), "string", "Año/fecha de construcción del edificio (INSPIRE 'beginning')."),
    ("currentUse","current_use", pa.string(), StringType(), "string", "Uso principal actual (1_residential, 2_agriculture, 3_industrial, 4_office, ...)."),
    ("numberOfBuildingUnits","num_units", pa.int32(), IntegerType(), "int", "Número de unidades constructivas (locales/inmuebles)."),
    ("numberOfDwellings","num_dwellings", pa.int32(), IntegerType(), "int", "Número de viviendas."),
    ("numberOfFloorsAboveGround","floors_above", pa.string(), StringType(), "string", "Número de plantas sobre rasante."),
    ("value","area_m2", pa.int32(), IntegerType(), "int", "Superficie construida en m² (INSPIRE 'value', uom m2)."),
 ],
 "parcelas": [
    ("nationalCadastralReference","reference", pa.string(), StringType(), "string", "Referencia catastral nacional de la parcela (refcat)."),
    ("localId","local_id", pa.string(), StringType(), "string", "Identificador local INSPIRE de la parcela."),
    ("label","label", pa.string(), StringType(), "string", "Etiqueta de la parcela (número de parcela)."),
    ("areaValue","area_m2", pa.int32(), IntegerType(), "int", "Superficie de la parcela en m² (INSPIRE 'areaValue', uom m2)."),
 ],
 "direcciones": [
    ("localId","local_id", pa.string(), StringType(), "string", "Identificador local INSPIRE de la dirección (portal)."),
    ("designator","designator", pa.string(), StringType(), "string", "Designador del portal: número de policía / referencia del acceso."),
    ("type","type", pa.int32(), IntegerType(), "int", "Tipo de localizador de dirección (código INSPIRE)."),
    ("level","level", pa.string(), StringType(), "string", "Nivel del componente de dirección."),
    ("specification","specification", pa.string(), StringType(), "string", "Especificación del punto de dirección (entrance, parcel, building...)."),
    ("method","method", pa.string(), StringType(), "string", "Método de geolocalización del portal."),
 ],
}
GEOM_DESC = "Geometría (EPSG:4326 / OGC:CRS84), tipo Geometry nativo de Parquet (GeoParquet 2.0). Snowflake/Iceberg podan sobre ella; DuckDB usa las columnas bbox."

def theme_columns(theme):
    """Devuelve la lista completa de columnas de salida en orden, con field_id asignado.
    geom va ANTES que bbox: así las columnas del esquema Iceberg (atributos+geom, sin bbox) tienen
    field-ids contiguos. Snowflake aborta la poda si el field-id de geom queda con huecos (bbox en medio)."""
    cols = list(THEMES[theme]) + COMMON_TAIL
    cols += [("geom","geom", GEOM_EXT, BinaryType(), "geometry", GEOM_DESC)]
    cols += [(c,c,at,it,jt,d) for (c,at,it,jt,d) in BBOX_COLS]
    # asigna field ids 1..n
    out=[]
    for i,(src,name,at,it,jt,desc) in enumerate(cols,1):
        out.append(dict(fid=i, src=src, name=name, atype=at, itype=it, jtype=jt, doc=desc))
    return out

def fmeta(fid, doc): return {b"PARQUET:field_id": str(fid).encode(), b"description": doc.encode()}

def reencode_province(theme, NN, cols):
    src = f"{SRC}/{theme}/provincia={NN}.parquet"
    raw = WORK / f"{theme}_raw_{NN}.parquet"
    fin = WORK / f"{theme}" / "data"; fin.mkdir(parents=True, exist_ok=True)
    finp = fin / f"provincia={NN}.parquet"
    # 1) DuckDB: selecciona columnas, geom->WKB, bbox plano
    sel=[]
    for c in cols:
        if c["name"]=="geom": sel.append("ST_AsWKB(geom) AS geom_wkb")
        elif c["name"] in ("xmin","ymin","xmax","ymax"): sel.append(f"bbox.{c['name']} AS {c['name']}")
        elif c["src"]==c["name"]: sel.append(c["src"])
        else: sel.append(f"{c['src']} AS {c['name']}")
    q=f"{DUCK_SECRET}COPY (SELECT {', '.join(sel)} FROM read_parquet('{src}')) TO '{raw}' (FORMAT parquet, COMPRESSION zstd);"
    r=subprocess.run(["duckdb","-unsigned","-c",q],capture_output=True,text=True)
    if not raw.exists(): raise RuntimeError(f"duckdb fallo {theme} {NN}: {r.stderr[-300:]}")
    # 2) pyarrow: re-encoda geom nativo + field metadata + descripciones
    t=pq.read_table(raw)
    arrays={}; fields=[]
    for c in cols:
        if c["name"]=="geom":
            arr=GEOM_EXT.wrap_array(t["geom_wkb"].combine_chunks())
            fields.append(pa.field("geom", GEOM_EXT, nullable=True, metadata=fmeta(c["fid"], c["doc"])))
        else:
            arr=t[c["name"]]
            # castea al tipo arrow deseado
            arr=pc.cast(arr.combine_chunks(), c["atype"]) if c["atype"]!=arr.type else arr.combine_chunks()
            fields.append(pa.field(c["name"], c["atype"], nullable=True, metadata=fmeta(c["fid"], c["doc"])))
        arrays[c["name"]]=arr
    schema=pa.schema(fields, metadata={b"catalog":b"catastro-es-portolan", b"theme":theme.encode()})
    tbl=pa.table({c["name"]:arrays[c["name"]] for c in cols}, schema=schema)
    pq.write_table(tbl, finp, compression="zstd", store_schema=True, write_statistics=True)
    raw.unlink()
    # 3) sube
    subprocess.run(["gcloud","storage","cp",str(finp),f"{GS}/v3/{theme}/data/provincia={NN}.parquet","-q"],
                   capture_output=True, check=True)
    # 4) bounds Iceberg por COLUMNA (todas) — Snowflake los exige en cada columna del esquema.
    #    string->UTF8(trunc), int->4-byte LE, double->8-byte LE, geom->packed_xy_le(bbox).
    #    Columna all-null -> placeholder (Snowflake aborta si falta el bound).
    nrows=tbl.num_rows
    def enc(jt,v):
        if v is None: return None
        if jt=="string": return str(v).encode("utf-8")[:60]
        if jt=="int":    return struct.pack("<i",int(v))
        if jt=="double": return struct.pack("<d",float(v))
        return None
    def ph(jt):
        return (b"",b"~") if jt=="string" else (struct.pack("<i",0),)*2 if jt=="int" else (struct.pack("<d",0.0),)*2
    gx0=pc.min(tbl["xmin"]).as_py(); gy0=pc.min(tbl["ymin"]).as_py()
    gx1=pc.max(tbl["xmax"]).as_py(); gy1=pc.max(tbl["ymax"]).as_py()
    lower={}; upper={}; vcounts={}; ncounts={}
    for c in cols:
        fid=c["fid"]; col=tbl[c["name"]]; vcounts[fid]=nrows; ncounts[fid]=int(col.null_count)
        if c["name"]=="geom":
            if None not in (gx0,gy0,gx1,gy1): lower[fid]=struct.pack("<dd",gx0,gy0); upper[fid]=struct.pack("<dd",gx1,gy1)
            else: lower[fid],upper[fid]=struct.pack("<dd",0.0,0.0),struct.pack("<dd",0.0,0.0)
            continue
        nn=col.drop_null()
        if len(nn)>0: lo=enc(c["jtype"],pc.min(nn).as_py()); hi=enc(c["jtype"],pc.max(nn).as_py())
        else: lo=hi=None
        if lo is None: lo,hi=ph(c["jtype"])
        lower[fid]=lo; upper[fid]=hi
    return dict(path=f"data/provincia={NN}.parquet", size=finp.stat().st_size, rows=nrows,
                lower={str(k):v.hex() for k,v in lower.items()},
                upper={str(k):v.hex() for k,v in upper.items()},
                value_counts=vcounts, null_value_counts=ncounts, NN=NN)

def main():
    theme=sys.argv[1]; cols=theme_columns(theme)
    provs=sys.argv[2:] or [l.strip() for l in open("/tmp/provincias.txt") if l.strip()]
    ledger=WORK/f"{theme}_datafiles.jsonl"
    done={json.loads(l)["NN"] for l in open(ledger)} if ledger.exists() else set()
    for NN in provs:
        if NN in done: print(f"  {theme} {NN}: ya hecho",flush=True); continue
        try:
            df=reencode_province(theme,NN,cols)
            with open(ledger,"a") as f: f.write(json.dumps(df)+"\n")
            print(f"  {theme} {NN}: OK {df['rows']} filas, {df['size']//1024}KB",flush=True)
        except Exception as e:
            print(f"  {theme} {NN}: ERROR {str(e)[:200]}",flush=True)
    n=len([1 for _ in open(ledger)]) if ledger.exists() else 0
    print(f"== {theme}: {n} provincias en ledger ==",flush=True)

if __name__=="__main__": main()
