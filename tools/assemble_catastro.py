import os, subprocess
OUT="/tmp/catout"
THEMES=[("edificios","/tmp/catbulk/parq",False),       # BU: sin cod_municipio inyectado
        ("parcelas","/tmp/catbulk_CP/parq",True),      # CP: con cod_municipio
        ("direcciones","/tmp/catbulk_AD/parq",True)]    # AD: con cod_municipio
def run(sql): 
    r=subprocess.run(["duckdb","-c",sql],capture_output=True,text=True); 
    if r.returncode: print("  ERR",r.stderr[-200:])
def rows(f):
    return subprocess.run(["duckdb","-noheader","-csv","-c",f"SELECT count(*) FROM read_parquet('{f}')"],capture_output=True,text=True).stdout.strip()
for name,parq,hascm in THEMES:
    raw=f"{OUT}/_{name}_raw.parquet"; gpq=f"{OUT}/_{name}_gpio.parquet"; fin=f"{OUT}/{name}.parquet"
    if hascm:
        sel="SELECT * FROM read_parquet('%s/*.parquet', union_by_name=true)"%parq
    else:
        sel=("SELECT * EXCLUDE(filename), regexp_extract(filename,'(\\d{5})\\.parquet',1) AS cod_municipio, '28' AS provincia "
             "FROM read_parquet('%s/*.parquet', union_by_name=true, filename=true)"%parq)
    run(f"INSTALL spatial;LOAD spatial; COPY ({sel}) TO '{raw}' (FORMAT parquet, COMPRESSION zstd);")
    subprocess.run(["gpio","convert",raw,gpq],capture_output=True,text=True)
    run(f"INSTALL spatial;LOAD spatial; COPY (SELECT * RENAME (geometry AS geom, geometry_bbox AS bbox) FROM read_parquet('{gpq}')) TO '{fin}' (FORMAT parquet, COMPRESSION zstd);")
    sz=os.path.getsize(fin)//1048576 if os.path.exists(fin) else 0
    print(f"{name}: {rows(fin)} features, {sz} MB -> {fin}",flush=True)
    for t in (raw,gpq):
        if os.path.exists(t): os.remove(t)
