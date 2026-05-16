#!/usr/bin/env python3
"""
bridge_tx.py — Chunk 4: SPPs into CCSDS TM Transfer Frames with FHP.

  - Listens on :50000 for Ref (impersonates the GDS).
  - Generates the 'sitting well' heartbeat toward Ref locally.
  - Chunks Ref's outgoing TCP stream into 1024-byte SPPs.
  - Packs SPPs into 256-byte TM Transfer Frames with FHP-driven boundaries.
  - Emits idle frames continuously when no data is available.
  - Each frame is prefixed with the 4-byte ASM (0x1ACFFC1D) on the wire.
"""
import collections
import signal
import socket
import struct
import threading
import time


# ─── Config ──────────────────────────────────────────────────────────────────
REF_LISTEN_HOST  = '0.0.0.0'
REF_LISTEN_PORT  = 50000

RX_HOST          = '100.64.56.2'
RX_PORT          = 52000

HEARTBEAT_BYTES  = b'sitting well'
HEARTBEAT_PERIOD = 0.5

RECV_BUF         = 4096

# F´ SPP parameters
SPP_SIZE         = 1024
SPP_SYNC         = (0x04, 0x42)

# CCSDS TM frame parameters
ASM              = bytes([0x1A, 0xCF, 0xFC, 0x1D])
TM_SCID          = 0x042
TM_VCID          = 0

FRAME_LEN        = 256
HEADER_LEN       = 6
FECF_LEN         = 2
DATA_FIELD_LEN   = FRAME_LEN - HEADER_LEN - FECF_LEN     # 248
OTA_LEN          = len(ASM) + FRAME_LEN                  # 260

FHP_NONE         = 0x7FF
FHP_IDLE         = 0x7FE

# Frame emission cadence when nothing is pending
IDLE_FRAME_PERIOD = 0.05    # 50ms; 20 idle frames/sec keeps the link active


# ─── CCSDS framing primitives ────────────────────────────────────────────────
def crc16_ccitt(data: bytes, init: int = 0xFFFF) -> int:
    crc = init
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def tm_primary_header(mcfc: int, vcfc: int, fhp: int) -> bytes:
    """6-byte TM Transfer Frame Primary Header per CCSDS 132.0-B-3 §4.1."""
    # Word 0: TFVN(2)=00 | SCID(10) | VCID(3) | OCF(1)=0
    w0 = ((TM_SCID & 0x3FF) << 4) | ((TM_VCID & 0x7) << 1)
    # Word 3: TFSH(1)=0 | Sync(1)=0 | PktOrder(1)=0 | SegLenID(2)=11 | FHP(11)
    w3 = (0b11 << 11) | (fhp & 0x7FF)
    return struct.pack('>HBBH', w0, mcfc & 0xFF, vcfc & 0xFF, w3)


def build_frame(data_field: bytes, mcfc: int, vcfc: int, fhp: int) -> bytes:
    """Build one OTA frame: ASM + Primary Hdr + Data Field + FECF."""
    assert len(data_field) == DATA_FIELD_LEN
    hdr = tm_primary_header(mcfc, vcfc, fhp)
    fecf = struct.pack('>H', crc16_ccitt(hdr + data_field))
    return ASM + hdr + data_field + fecf


# ─── Fixed-size SPP parser (Chunk 3 v2) ──────────────────────────────────────
class FixedSizeParser:
    def __init__(self):
        self._buf = bytearray()

    def feed(self, data: bytes):
        self._buf.extend(data)

    def pop(self):
        while len(self._buf) >= SPP_SIZE:
            pkt = bytes(self._buf[:SPP_SIZE])
            del self._buf[:SPP_SIZE]
            if pkt[0] != SPP_SYNC[0] or pkt[1] != SPP_SYNC[1]:
                print(f"[TX] WARN: packet start {pkt[0]:02x} {pkt[1]:02x} "
                      f"!= 04 42, possible sync loss")
            yield pkt


# ─── SPP queue: thread-safe, used by both the TCP thread and the framer ─────
class SppQueue:
    def __init__(self):
        self._q = collections.deque()
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)

    def push(self, spp: bytes):
        with self._cond:
            self._q.append(spp)
            self._cond.notify()

    def pop_nowait(self) -> bytes | None:
        with self._lock:
            return self._q.popleft() if self._q else None

    def __len__(self):
        with self._lock:
            return len(self._q)


# ─── Frame producer: pulls SPPs, emits frames continuously ──────────────────
class FrameProducer:
    """
    State: holds the currently-emitting SPP and its bit-offset.
    Each frame: try to write 248 bytes from current SPP + queued SPPs.
    Set FHP to the offset where the next SPP header lands in this frame,
    or FHP_NONE if no SPP starts in this frame.
    """
    def __init__(self, queue: SppQueue):
        self._q          = queue
        self._current    = b''       # SPP being emitted
        self._cur_off    = 0         # bytes already emitted from _current
        self._mcfc       = 0
        self._vcfc       = 0
        self._n_data     = 0
        self._n_idle     = 0
        self._lock       = threading.Lock()

    @property
    def stats(self):
        with self._lock:
            return self._n_data, self._n_idle

    def _build_data_field(self) -> tuple[bytes, int]:
        """
        Returns (data_field_bytes, fhp).
        Greedily fills 248 bytes from the current SPP and the queue.
        FHP = offset of first NEW SPP header in this frame, or FHP_NONE.
        """
        df = bytearray()
        fhp = FHP_NONE  # default: no new SPP starts here

        while len(df) < DATA_FIELD_LEN:
            # Need a new SPP?
            if self._cur_off >= len(self._current):
                nxt = self._q.pop_nowait()
                if nxt is None:
                    break
                self._current = nxt
                self._cur_off = 0
                if fhp == FHP_NONE:
                    fhp = len(df)   # first new SPP starts at this byte

            take = min(DATA_FIELD_LEN - len(df),
                       len(self._current) - self._cur_off)
            df.extend(self._current[self._cur_off:self._cur_off + take])
            self._cur_off += take

        # Pad with zeros if data field underflows (queue empty mid-frame)
        if len(df) < DATA_FIELD_LEN:
            df.extend(b'\x00' * (DATA_FIELD_LEN - len(df)))

        return bytes(df), fhp

    def next_frame(self) -> bytes:
        """
        Produce one OTA frame. Idle frame iff no SPP is in progress AND the
        queue is empty.
        """
        with self._lock:
            queue_empty = (self._cur_off >= len(self._current)) and (len(self._q) == 0)
            if queue_empty:
                df  = b'\x00' * DATA_FIELD_LEN
                fhp = FHP_IDLE
                self._n_idle += 1
            else:
                df, fhp = self._build_data_field()
                self._n_data += 1
            frame = build_frame(df, self._mcfc, self._vcfc, fhp)
            self._mcfc = (self._mcfc + 1) & 0xFF
            self._vcfc = (self._vcfc + 1) & 0xFF
            return frame


# ─── Inter-bridge link (with reconnect) ──────────────────────────────────────
class RxLink:
    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port
        self._sock: socket.socket | None = None
        self._lock = threading.Lock()

    def connect(self, stop_event: threading.Event):
        while not stop_event.is_set():
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self._host, self._port))
                with self._lock:
                    self._sock = s
                print(f"[TX] Connected to bridge_rx at {self._host}:{self._port}")
                return
            except (ConnectionRefusedError, OSError) as e:
                print(f"[TX] bridge_rx not ready ({e}), retrying in 2s")
                time.sleep(2)

    def send(self, data: bytes) -> bool:
        with self._lock:
            s = self._sock
        if s is None:
            return False
        try:
            s.sendall(data)
            return True
        except (BrokenPipeError, ConnectionResetError, OSError) as e:
            print(f"[TX] Send to bridge_rx failed: {e}")
            with self._lock:
                if self._sock is s:
                    self._sock.close()
                    self._sock = None
            return False

    def close(self):
        with self._lock:
            if self._sock:
                self._sock.close()
                self._sock = None


# ─── Frame emission loop ─────────────────────────────────────────────────────
def emit_frames(producer: FrameProducer, rx: RxLink, stop_event: threading.Event):
    last_idle = 0.0
    while not stop_event.is_set():
        # If queue is empty, throttle idle frames to a reasonable rate
        if len(producer._q) == 0 and (producer._cur_off >= len(producer._current)):
            now = time.monotonic()
            if now - last_idle < IDLE_FRAME_PERIOD:
                time.sleep(0.005)
                continue
            last_idle = time.monotonic()

        frame = producer.next_frame()
        if not rx.send(frame):
            threading.Thread(target=rx.connect, args=(stop_event,),
                             daemon=True).start()
            time.sleep(0.5)


# ─── Per-Ref-connection handler ──────────────────────────────────────────────
def handle_ref(conn: socket.socket, addr, queue: SppQueue,
               stop_event: threading.Event):
    print(f"[TX] Ref connected from {addr}")
    conn.settimeout(0.1)

    parser    = FixedSizeParser()
    bytes_in  = 0
    spp_count = 0
    last_hb   = 0.0

    try:
        while not stop_event.is_set():
            now = time.monotonic()
            if now - last_hb >= HEARTBEAT_PERIOD:
                try:
                    conn.sendall(HEARTBEAT_BYTES)
                    last_hb = now
                except (BrokenPipeError, ConnectionResetError, OSError) as e:
                    print(f"[TX] Heartbeat send failed: {e}")
                    break

            try:
                data = conn.recv(RECV_BUF)
                if not data:
                    print("[TX] Ref closed the connection.")
                    break
                bytes_in += len(data)
                parser.feed(data)
                for spp in parser.pop():
                    queue.push(spp)
                    spp_count += 1
                    if spp_count <= 5 or spp_count % 100 == 0:
                        apid = ((spp[0] << 8) | spp[1]) & 0x07FF
                        print(f"[TX] SPP #{spp_count}: APID=0x{apid:03X} "
                              f"seq={spp[2]} len={len(spp)}B "
                              f"(queue={len(queue)})")
            except socket.timeout:
                pass
            except (ConnectionResetError, OSError) as e:
                print(f"[TX] Ref recv failed: {e}")
                break
    finally:
        conn.close()
        print(f"[TX] Session ended. bytes_in={bytes_in}B  spps_queued={spp_count}")


def serve_ref(queue: SppQueue, stop_event: threading.Event):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((REF_LISTEN_HOST, REF_LISTEN_PORT))
    srv.listen(1)
    srv.settimeout(1.0)
    print(f"[TX] Listening for Ref on {REF_LISTEN_HOST}:{REF_LISTEN_PORT}")

    try:
        while not stop_event.is_set():
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                continue
            handle_ref(conn, addr, queue, stop_event)
    finally:
        srv.close()


def main():
    stop_event = threading.Event()
    signal.signal(signal.SIGINT,  lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    print("=== bridge_tx.py — Chunk 4: TM frames with FHP ===")
    print(f"    OTA frame:    ASM(4) + TF({FRAME_LEN}) = {OTA_LEN}B")
    print(f"    Data field:   {DATA_FIELD_LEN}B per frame")
    print(f"    bridge_rx:    {RX_HOST}:{RX_PORT}")

    queue    = SppQueue()
    rx       = RxLink(RX_HOST, RX_PORT)
    producer = FrameProducer(queue)

    rx.connect(stop_event)
    if stop_event.is_set():
        return

    # Frame emitter runs in the background
    emitter = threading.Thread(
        target=emit_frames, args=(producer, rx, stop_event), daemon=True)
    emitter.start()

    # Periodic stats
    def stats_loop():
        t0 = time.time()
        while not stop_event.is_set():
            stop_event.wait(timeout=5.0)
            data_n, idle_n = producer.stats
            print(f"[TX] [{time.time()-t0:6.0f}s] data_frames={data_n} "
                  f"idle_frames={idle_n} queue_depth={len(queue)}")
    threading.Thread(target=stats_loop, daemon=True).start()

    serve_ref(queue, stop_event)
    rx.close()
    print("[TX] Stopped.")


if __name__ == '__main__':
    main()