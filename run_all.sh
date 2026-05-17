#!/bin/bash
# run_all.sh — orchestrate the full pipeline

set -euo pipefail

SESSION=fprime

GDS_HOST=100.64.56.2
GDS_HOST_DIR='/data/sdr-bridge'         # path on GDS host
GDS_DICT_DIR='/data/gds/fprime-gds'                # where the dictionary lives
FSW_BRIDGE_DIR=/home/ethant/Projects/sdr-bridge
FSW_REF_DIR=/home/ethant/Projects/fprime

# Reachability check
if ! ssh -o BatchMode=yes -o ConnectTimeout=3 "$GDS_HOST" 'true'; then
  echo "Can't SSH to $GDS_HOST without prompting. Fix Tailscale SSH or key auth first."
  exit 1
fi

echo "Starting GDS on remote host..."
ssh -t "$GDS_HOST" "cd $GDS_DICT_DIR && source .venv/bin/activate && fprime-gds -n -g html --gui-addr 0.0.0.0 --ip-address 0.0.0.0 --dictionary ../dict/RefTopologyDictionary.json --persistent-db" &
P1=$!

echo "Starting bridge_rx on remote host in 3s..."
(sleep 3 && ssh -t "$GDS_HOST" "cd $GDS_HOST_DIR && python3 gds_rx.py") &
P2=$!

echo "Starting bridge_tx locally in 5s..."
(sleep 5 && cd "$FSW_BRIDGE_DIR" && python3 gds_tx.py) &
P3=$!

echo "Starting Ref locally in 7s..."
(sleep 7 && cd "$FSW_REF_DIR" && sudo ./Ref/build-artifacts/Linux/Ref/bin/Ref -a 127.0.0.1 -p 50000) &
P4=$!

cleanup() {
    echo "Stopping all processes and closing ports..."
    kill $P1 $P2 $P3 $P4 2>/dev/null || true
    lsof -t -i :50000 2>/dev/null | xargs -r kill -9 2>/dev/null || true
    ssh -o ConnectTimeout=3 "$GDS_HOST" "lsof -t -i :5000 -i :50000 -i :52000 2>/dev/null | xargs -r kill -9 2>/dev/null || true"
}

trap cleanup EXIT INT TERM

echo "All processes started. Press Ctrl+C to stop all."

wait
