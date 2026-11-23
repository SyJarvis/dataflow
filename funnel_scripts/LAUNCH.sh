#!/bin/bash
set -euo pipefail
FUNNEL="$HOME/dataflow_prep_20261111/funnel"
mkdir -p "$FUNNEL"
SRC="$FUNNEL/scripts"
mkdir -p "$SRC"
cp -f scan_tars.py extract_meta.py analyze_dedup.py "$SRC/" 2>/dev/null || true
cd "$FUNNEL"

# copy scripts from this dir if launched from scripts folder
HERE="$(cd "$(dirname "$0")" && pwd)"
cp -f "$HERE"/scan_tars.py "$HERE"/extract_meta.py "$HERE"/analyze_dedup.py "$FUNNEL/" || true

echo "starting extract_meta and scan_tars at $(date)"
nohup python3 -u "$FUNNEL/extract_meta.py" --workers 16 > "$FUNNEL/extract_meta.log" 2>&1 &
echo $! > "$FUNNEL/extract_meta.pid"
nohup python3 -u "$FUNNEL/scan_tars.py" --workers 32 > "$FUNNEL/scan_tars.log" 2>&1 &
echo $! > "$FUNNEL/scan_tars.pid"
echo "extract pid $(cat $FUNNEL/extract_meta.pid)"
echo "scan pid $(cat $FUNNEL/scan_tars.pid)"
