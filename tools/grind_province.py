#!/usr/bin/env python3
"""Catastro INSPIRE -> GeoParquet canónico por PROVINCIA y tema. Resumable.
Descarga+convierte municipios (foreground; -k cert FNMT, %20 URLs), une, inyecta cod_municipio+provincia,
gpio convert (bbox+Hilbert+ZSTD), renombra a geom/bbox. Salida: /tmp/catout_es/{tema}__{PROV}.parquet
Uso: prov.py PROV [BU,CP,AD]   (por defecto los 3 temas)
"""
import os, re, sys, subprocess, concurrent.futures as cf
PROV=sys.argv[1]
THEMES=(sys.argv[2].split(",") if len(sys.argv)>2 else ["BU","CP","AD"])
MAP={"BU":("Buildings","Building","building"),"CP":("CadastralParcels","CadastralParcel","cadastralparcel"),
     "AD":("Addresses","Address","")}
OUT="/tmp/catout_es"; os.makedirs(OUT,exist_ok=True)

def fetch_urls(theme):
    dirn=MAP[theme][0]
    atom=f"https://www.catastro.hacienda.gob.es/INSPIRE/{dirn}/{PROV}/ES.SDGC.{theme}.atom_{PROV}.xml"
    f=f"/tmp/_atom_{theme}_{PROV}.xml"
    subprocess.run(["curl","-k","-s","--max-time","90","-o",f,atom])
    try: x=open(f,encoding="iso-8859-1").read()
    except Exception: return []
    return re.findall(r'rel="enclosure" href="([^"]+\.zip)"', x)

def conv_theme(theme):
    dirn,layer,suffix=MAP[theme]
    urls=fetch_urls(theme)
    work=f"/tmp/cb_{theme}_{PROV}"; parq=f"{work}/parq"; os.makedirs(parq,exist_ok=True)
    def code(u):
        m=re.search(rf'{theme}\.(\d+)\.zip',u); return m.group(1) if m else u.split('/')[-1]
    def one(u):
        c=code(u); out=f"{parq}/{c}.parquet"
        if os.path.exists(out) and os.path.getsize(out)>500: return "skip"
        z=f"{work}/{c}.zip"; d=f"{work}/{c}"
        try:
            import time as t
            for a in range(3):
                subprocess.run(["curl","-k","-s","--retry","2","--max-time","420","-o",z,u.replace(" ","%20")],capture_output=True)
                if os.path.exists(z) and os.path.getsize(z)>=200: break
                t.sleep(2+a*3)
            if not os.path.exists(z) or os.path.getsize(z)<200: return "dl"
            os.makedirs(d,exist_ok=True); subprocess.run(["unzip","-o","-q","-j",z,"-d",d],capture_output=True)
            g=[x for x in os.listdir(d) if x.lower().endswith(suffix+".gml")]
            if not g: return "nogml"
            sql=f"SELECT *, '{c}' AS cod_municipio, '{PROV}' AS provincia FROM {layer}"
            subprocess.run(["ogr2ogr","-f","Parquet",out,os.path.join(d,g[0]),"-sql",sql,"-t_srs","EPSG:4326",
                "-lco","COMPRESSION=ZSTD","-lco","GEOMETRY_ENCODING=WKB"],capture_output=True)
            for x in os.listdir(d): os.remove(os.path.join(d,x))
            os.rmdir(d); os.remove(z)
            return "ok" if os.path.exists(out) and os.path.getsize(out)>500 else "conv"
        except Exception: return "err"
    done={f[:-8] for f in os.listdir(parq) if f.endswith(".parquet")}
    todo=[u for u in urls if code(u) not in done]
    res={}
    with cf.ThreadPoolExecutor(max_workers=3) as ex:
        for st in ex.map(one,todo): res[st]=res.get(st,0)+1
    have=len([f for f in os.listdir(parq) if f.endswith(".parquet")])
    print(f"  {theme} prov {PROV}: {res} | {have}/{len(urls)} munis",flush=True)
    return have, len(urls), parq

def assemble(theme, parq):
    name={"BU":"edificios","CP":"parcelas","AD":"direcciones"}[theme]
    fin=f"{OUT}/{name}__{PROV}.parquet"
    raw=f"{OUT}/_raw_{theme}_{PROV}.parquet"; gpq=f"{OUT}/_gpio_{theme}_{PROV}.parquet"
    subprocess.run(["duckdb","-c",f"INSTALL spatial;LOAD spatial;COPY (SELECT * FROM read_parquet('{parq}/*.parquet', union_by_name=true)) TO '{raw}' (FORMAT parquet,COMPRESSION zstd);"],capture_output=True)
    subprocess.run(["gpio","convert",raw,gpq],capture_output=True)
    subprocess.run(["duckdb","-c",f"INSTALL spatial;LOAD spatial;COPY (SELECT * RENAME (geometry AS geom, geometry_bbox AS bbox) FROM read_parquet('{gpq}')) TO '{fin}' (FORMAT parquet,COMPRESSION zstd);"],capture_output=True)
    for t in (raw,gpq):
        if os.path.exists(t): os.remove(t)
    n=subprocess.run(["duckdb","-noheader","-csv","-c",f"SELECT count(*) FROM read_parquet('{fin}')"],capture_output=True,text=True).stdout.strip()
    print(f"  -> {name}__{PROV}: {n} features",flush=True)
    return fin, n

print(f"== PROVINCIA {PROV} ==",flush=True)
for th in THEMES:
    have,tot,parq=conv_theme(th)
    if have==tot and tot>0: assemble(th,parq)
    else: print(f"  {th}: INCOMPLETO {have}/{tot} (re-ejecuta para continuar)",flush=True)
