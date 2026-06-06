#!/usr/bin/env python3
# Descarga+convierte un tema INSPIRE de Catastro (provincia 28 Madrid) -> parquet por municipio.
# Temas: BU=Buildings/Building, CP=CadastralParcels/CadastralParcel, AD=Addresses/Address
# Soluciona cert FNMT (-k) y espacios en URL (%20). Resumable. Uso: cat_bulk.py BU|CP|AD [limit]
import os, re, sys, subprocess, concurrent.futures as cf, urllib.request, ssl
THEME=sys.argv[1] if len(sys.argv)>1 else "BU"
LIMIT=int(sys.argv[2]) if len(sys.argv)>2 else 9999
MAP={"BU":("Buildings","Building","building"),
     "CP":("CadastralParcels","CadastralParcel","cadastralparcel"),
     "AD":("Addresses","Address","")}
DIRN,LAYER,SUFFIX=MAP[THEME]
ATOM=f"https://www.catastro.hacienda.gob.es/INSPIRE/{DIRN}/28/ES.SDGC.{THEME}.atom_28.xml"
WORK=f"/tmp/catbulk_{THEME}"; PARQ=f"{WORK}/parq"; os.makedirs(PARQ,exist_ok=True)

# bajar ATOM provincia (con -k)
atomf=f"{WORK}/atom.xml"
subprocess.run(["curl","-k","-s","--max-time","90","-o",atomf,ATOM])
x=open(atomf,encoding="iso-8859-1").read()
URLS=re.findall(r'rel="enclosure" href="([^"]+\.zip)"', x)
print(f"[{THEME}] municipios en ATOM: {len(URLS)}",flush=True)

def code(u):
    m=re.search(rf'{THEME}\.(\d+)\.zip', u); return m.group(1) if m else u.split('/')[-1]
def one(u):
    c=code(u); out=f"{PARQ}/{c}.parquet"
    if os.path.exists(out) and os.path.getsize(out)>500: return c,"skip"
    z=f"{WORK}/{c}.zip"; d=f"{WORK}/{c}"
    try:
        import time as t
        for a in range(3):
            subprocess.run(["curl","-k","-s","--retry","2","--max-time","420","-o",z,u.replace(" ","%20")],capture_output=True)
            if os.path.exists(z) and os.path.getsize(z)>=200: break
            t.sleep(3+a*4)
        if not os.path.exists(z) or os.path.getsize(z)<200: return c,"FAIL-dl"
        os.makedirs(d,exist_ok=True)
        subprocess.run(["unzip","-o","-q","-j",z,"-d",d],capture_output=True)
        gml=[f for f in os.listdir(d) if f.lower().endswith(SUFFIX+".gml")]
        if not gml: return c,"no-gml"
        # inyecta cod_municipio + provincia con OGR SQL
        sql=f"SELECT *, '{c}' AS cod_municipio, '28' AS provincia FROM {LAYER}"
        cp=subprocess.run(["ogr2ogr","-f","Parquet",out,os.path.join(d,gml[0]),"-dialect","OGRSQL","-sql",sql,
            "-lco","COMPRESSION=ZSTD","-lco","GEOMETRY_ENCODING=WKB"],capture_output=True,text=True)
        for f in os.listdir(d): os.remove(os.path.join(d,f))
        os.rmdir(d); os.remove(z)
        return c,("OK" if os.path.exists(out) and os.path.getsize(out)>500 else f"FAIL:{cp.stderr.strip()[:80]}")
    except Exception as e: return c,f"ERR:{str(e)[:80]}"

done={f[:-8] for f in os.listdir(PARQ) if f.endswith(".parquet")}
todo=[u for u in URLS if code(u) not in done][:LIMIT]
ok=skip=fail=0; fails=[]
with cf.ThreadPoolExecutor(max_workers=3) as ex:
    for i,(c,st) in enumerate(ex.map(one,todo),1):
        if st=="OK":ok+=1
        elif st=="skip":skip+=1
        else:fail+=1;fails.append((c,st))
        if i%10==0 or not st.startswith(("OK","skip")): print(f"[{i}/{len(todo)}] {st:12s} {c} (ok={ok} fail={fail})",flush=True)
print(f"[{THEME}] PASS: ok={ok} skip={skip} fail={fail}; total={len(os.listdir(PARQ))}/{len(URLS)}",flush=True)
for c,s in fails[:15]: print("  FAIL",c,s,flush=True)
