#!/usr/bin/env bash
# Reproduces every number in the README from a cold start.
#
#   ./run-drill.sh
#
# Takes roughly 6-8 minutes. The write-queue restart in step 4 is why this is a
# script rather than a single Python entry point: forcing real bulk rejections
# needs a differently-configured Elasticsearch, and the drill refuses to fake
# them.
set -euo pipefail

PY=${PY:-./.venv/bin/python}
ROWS=${ROWS:-500000}
WINDOW=${WINDOW:-12}

if [[ ! -x "$PY" ]]; then
  echo "==> creating venv"
  python3 -m venv .venv
  ./.venv/bin/pip -q install 'elasticsearch>=8,<9' 'psycopg[binary]'
fi

echo
echo "############ 1/5  stdlib demo (no containers needed)"
python3 reindex_drill.py

echo
echo "############ 2/5  bringing up postgres + 2-node elasticsearch"
./podman-setup.sh

echo
echo "############ 2b/5  regression tests (failure paths)"
"$PY" -m drill.selftest

echo
echo "############ 3/5  run matrix (5 configs, reseeded before each)"
"$PY" -m drill.report --all --window "$WINDOW" --rows "$ROWS"

echo
echo "############ 4/5  commit-time watermark proof"
"$PY" -m drill.commit_time_demo

echo
echo "############ 5/5  real bulk rejections (constrained write queue)"
./podman-setup.sh --small-queue
"$PY" -m drill.report --probe-rejections

echo
echo "############ final results"
"$PY" -m drill.report

echo
echo "Raw per-run JSON is in results/."
echo "Tear down with: podman pod rm -f drill"
