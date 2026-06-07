#!/usr/bin/env bash
#
# run_all.sh — start the full F´ SDR-bridge stack across two Tailscale hosts.
#
# Topology:
#   GDS host (remote, via Tailscale):
#     - fprime-gds   on :50000
#     - bridge_rx.py : RTL-SDR -> demod -> deframe -> GDS
#   FSW host (local):
#     - bridge_tx.py : Ref TCP -> SPPs -> TM frames -> GMSK -> PlutoSDR
#     - Ref binary
#
# Startup order (bottom-of-stack first):
#   0. Verify Tailscale tunnel is up
#   1. GDS
#   2. bridge_rx
#   3. bridge_tx
#   4. Ref
#
set -euo pipefail

# ─── Config ──────────────────────────────────────────────────────────────────
# Prefer MagicDNS hostname (no DNS surprises if Tailscale reassigns CGNAT IP).
# Use `tailscale status` to find the short name (e.g. "gds-host"), or use the
# fully-qualified name (e.g. "gds-host.tail-scale.ts.net").
GDS_HOST=${GDS_HOST:-100.64.56.2}              # Tailscale MagicDNS name
GDS_USER=${GDS_USER:-ethant}
GDS_REMOTE_DIR=${GDS_REMOTE_DIR:-/data/gds}

REF_BIN=${REF_BIN:-/home/ethant/Projects/fprime/Ref/build-artifacts/Linux/Ref/bin/Ref}
DICT_PATH=${DICT_PATH:-/data/gds/dict/RefTopologyDictionary.json}

LOG_DIR=${LOG_DIR:-./logs}
mkdir -p "$LOG_DIR"

# ─── Pre-flight: clean up any leftover processes from a previous run ─────────
echo "Cleaning up any leftover processes on ${GDS_HOST}..."
ssh -n "${GDS_USER}@${GDS_HOST}" \
    "pkill -f 'fprime-gds|bridge_rx.py' 2>/dev/null; sleep 1; \
     pkill -9 -f 'fprime-gds|bridge_rx.py' 2>/dev/null || true" || true
# Local side too — in case bridge_tx is still hanging around
pkill -f 'bridge_tx.py' 2>/dev/null || true
pkill -f "Ref/build-artifacts" 2>/dev/null || true
sleep 1

# ─── Tailscale tunnel sanity check ───────────────────────────────────────────
echo "Checking Tailscale tunnel..."
if ! command -v tailscale >/dev/null 2>&1; then
    echo "  Tailscale CLI not found. Is the daemon installed?"
    exit 1
fi
if ! tailscale status >/dev/null 2>&1; then
    echo "  Tailscale is installed but not running. Try: sudo tailscale up"
    exit 1
fi
# Resolve GDS host through Tailscale and confirm reachability
GDS_IP=$(tailscale ip -4 "${GDS_HOST}" 2>/dev/null | head -n1 || true)
if [[ -z "${GDS_IP}" ]]; then
    echo "  Could not resolve '${GDS_HOST}' on the tailnet."
    echo "  Hosts visible to this node:"
    tailscale status | awk '{print "    " $0}'
    exit 1
fi
echo "  ${GDS_HOST} -> ${GDS_IP} (tailnet)"

# ─── Prime sudo so the backgrounded Ref launch doesn't deadlock on a prompt ──
echo "Caching sudo credentials for the Ref binary..."
sudo -v
# Keep the sudo timestamp alive for the duration of the script
( while true; do sudo -n true; sleep 60; kill -0 "$$" 2>/dev/null || exit; done ) &
SUDO_KEEPALIVE_PID=$!

# ─── Cleanup on Ctrl-C ───────────────────────────────────────────────────────
PIDS=()
cleanup() {
    # Prevent re-entry if a second signal arrives during cleanup
    trap '' INT TERM

    echo
    echo "Stopping all processes..."

    # 1. Kill the sudo keepalive loop first
    kill "$SUDO_KEEPALIVE_PID" 2>/dev/null || true

    # 2. Kill Ref (runs as root, so needs sudo kill)
    sudo pkill -f "Ref/build-artifacts" 2>/dev/null || true

    # 3. TERM all tracked local children (SSH wrappers, bridge_tx, sudo)
    for pid in "${PIDS[@]}"; do
        kill "$pid" 2>/dev/null || true
    done
    sleep 1
    # Force-kill anything that didn't exit
    for pid in "${PIDS[@]}"; do
        kill -9 "$pid" 2>/dev/null || true
    done

    # 4. Remote children — timeout the SSH so it can't block us
    ssh -o ConnectTimeout=3 -o ServerAliveInterval=2 -o ServerAliveCountMax=2 \
        -n "${GDS_USER}@${GDS_HOST}" \
        "pkill -f 'fprime-gds|bridge_rx.py' 2>/dev/null; sleep 1; \
         pkill -9 -f 'fprime-gds|bridge_rx.py' 2>/dev/null || true" 2>/dev/null || true

    # 5. Reap children with a bounded wait (don't block forever)
    local deadline=$((SECONDS + 5))
    while jobs -p | grep -q . && (( SECONDS < deadline )); do
        wait -n 2>/dev/null || true
    done
    # Force-kill anything still lingering
    for straggler in $(jobs -p 2>/dev/null); do
        kill -9 "$straggler" 2>/dev/null || true
    done

    echo "Done."
    exit 0
}
trap cleanup INT TERM

# ─── Port-wait helper (uses bash /dev/tcp, no nc needed) ─────────────────────
wait_for_port() {
    local host=$1 port=$2 timeout=${3:-20}
    for ((i=0; i<timeout; i++)); do
        if (echo > /dev/tcp/${host}/${port}) 2>/dev/null; then
            return 0
        fi
        sleep 1
    done
    return 1
}

# ─── Start GDS (remote) ──────────────────────────────────────────────────────
echo "Starting fprime-gds on ${GDS_HOST}..."
ssh -n "${GDS_USER}@${GDS_HOST}" \
    "cd ${GDS_REMOTE_DIR} && \
     source ./fprime-gds/.venv/bin/activate && \
     fprime-gds -n -g html --gui-addr 0.0.0.0 \
       --ip-address 0.0.0.0 \
       --dictionary ${DICT_PATH} \
       --persistent-db" \
    > "${LOG_DIR}/gds.log" 2>&1 &
PIDS+=($!)

if ! wait_for_port "${GDS_HOST}" 50000 25; then
    echo "  GDS did not open :50000 within 25s. See ${LOG_DIR}/gds.log"
    cleanup
fi
echo "  GDS listening."

# ─── Start bridge_rx (remote) ────────────────────────────────────────────────
echo "Starting bridge_rx on ${GDS_HOST}..."
ssh -n "${GDS_USER}@${GDS_HOST}" \
    "cd /data/sdr-bridge && python3 bridge_rx.py" \
    > "${LOG_DIR}/bridge_rx.log" 2>&1 &
PIDS+=($!)
sleep 3   # bridge_rx has no listening port; just give it a moment to attach RTL-SDR

# ─── Start bridge_tx (local) ─────────────────────────────────────────────────
echo "Starting bridge_tx locally..."
python3 bridge_tx.py > "${LOG_DIR}/bridge_tx.log" 2>&1 &
PIDS+=($!)

if ! wait_for_port 127.0.0.1 50000 15; then
    echo "  bridge_tx did not open :50000 within 15s. See ${LOG_DIR}/bridge_tx.log"
    cleanup
fi
echo "  bridge_tx listening."

# ─── Start Ref (local) ───────────────────────────────────────────────────────
echo "Starting Ref locally..."
sudo "${REF_BIN}" -a 127.0.0.1 -p 50000 \
    > "${LOG_DIR}/ref.log" 2>&1 &
PIDS+=($!)

# ─── Summary and wait ────────────────────────────────────────────────────────
echo
echo "Stack up. Logs in ${LOG_DIR}/"
echo "  gds.log         (remote: ${GDS_HOST})"
echo "  bridge_rx.log   (remote: ${GDS_HOST})"
echo "  bridge_tx.log   (local)"
echo "  ref.log         (local)"
echo
echo "GDS GUI: http://${GDS_HOST}:5000     (or check ${LOG_DIR}/gds.log for the actual port)"
echo "Tail any log:  tail -f ${LOG_DIR}/<name>.log"
echo
echo "Press Ctrl+C to stop everything."

# Block until any child exits, then tear down
wait -n || true
echo
echo "A child process exited unexpectedly. Tearing down."
cleanup