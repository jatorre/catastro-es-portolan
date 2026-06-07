#!/bin/bash
# Re-encode + sube + metadata para los 3 temas, todas las provincias. Resumable.
PY=/Users/jatorre/workspace/iceberg-geo-testbed/.venv/bin/python
cd /tmp
for theme in edificios direcciones parcelas; do
  echo "########## TEMA $theme $(date +%H:%M:%S) ##########"
  $PY /tmp/cat_v3_build.py "$theme"
  n=$( [ -f /tmp/v3work/${theme}_datafiles.jsonl ] && wc -l < /tmp/v3work/${theme}_datafiles.jsonl || echo 0 )
  echo ">>> $theme: $n / 52 data files"
  if [ "$n" -ge 52 ]; then
    echo ">>> escribiendo metadata v3 de $theme"
    $PY /tmp/cat_v3_meta.py "$theme"
  else
    echo ">>> $theme incompleto ($n/52), metadata pendiente"
  fi
done
echo "########## GRIND V3 FIN $(date +%H:%M:%S) ##########"
