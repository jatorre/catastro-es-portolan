#!/bin/bash
cd /tmp
for NN in $(cat /tmp/provincias.txt); do
  grep -qx "$NN" /tmp/catout_es/_done.txt && continue
  echo "===== PROV $NN $(date +%H:%M:%S) =====" 
  python3 /tmp/prov.py "$NN" 2>&1
  up=0
  for name in edificios parcelas direcciones; do
    f="/tmp/catout_es/${name}__${NN}.parquet"
    if [ -f "$f" ]; then gcloud storage cp "$f" "gs://catastro-es-portolan/data/parquet/${name}/provincia=${NN}.parquet" 2>/dev/null && up=$((up+1)); fi
  done
  if [ "$up" = "3" ]; then echo "$NN" >> /tmp/catout_es/_done.txt; echo ">>> PROV $NN OK ($(wc -l </tmp/catout_es/_done.txt) provincias)"; rm -rf /tmp/cb_*_${NN} /tmp/catout_es/*__${NN}.parquet; else echo ">>> PROV $NN PARCIAL ($up/3) — se reintenta en otra pasada"; fi
done
echo "===== GRIND FIN: $(wc -l </tmp/catout_es/_done.txt) provincias ====="
