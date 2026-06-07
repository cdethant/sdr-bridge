#!/usr/bin/env python3
"""
bridge_tx.py — Chunk 5 (patched): SPPs into CCSDS TM frames, GMSK over PlutoSDR.

  - Listens on :50000 for Ref (impersonates the GDS, no heartbeat needed).
  - Chunks Ref's outgoing TCP stream into 1024-byte SPPs.
  - Packs SPPs into 256-byte TM Transfer Frames with FHP.
  - Emits idle frames continuously when no data is queued.
  - Internal GR flowgraph: FrameSource -> gmsk_mod -> PlutoSDR sink.
    (No intra-process TCP loop; FrameSource pulls from FrameProducer directly.)
"""
import collections
import signal
import socket
import struct
import threading
import time

import numpy as np
from gnuradio import gr, digital
from gnuradio import iio


# ─── Config ──────────────────────────────────────────────────────────────────
REF_LISTEN_HOST  = '0.0.0.0'
REF_LISTEN_PORT  = 50000

CENTER_FREQ      = 433_000_000
SAMP_RATE        = 2_000_000
SPS              = 4
BT               = 0.35
TX_ATTENUATION   = 20.0

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
    w0 = ((TM_SCID & 0x3FF) << 4) | ((TM_VCID & 0x7) << 1)
    w3 = (0b11 << 11) | (fhp & 0x7FF)
    return struct.pack('>HBBH', w0, mcfc & 0xFF, vcfc & 0xFF, w3)


def build_frame(data_field: bytes, mcfc: int, vcfc: int, fhp: int) -> bytes:
    """Build one OTA frame: ASM + Primary Hdr + Data Field + FECF."""
    assert len(data_field) == DATA_FIELD_LEN
    hdr  = tm_primary_header(mcfc, vcfc, fhp)
    fecf = struct.pack('>H', crc16_ccitt(hdr + data_field))
    return ASM + hdr + data_field + fecf


# ─── Fixed-size SPP parser ───────────────────────────────────────────────────
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


# ─── SPP queue ───────────────────────────────────────────────────────────────
class SppQueue:
    def __init__(self):
        self._q = collections.deque()
        self._lock = threading.Lock()

    def push(self, spp: bytes):
        with self._lock:
            self._q.append(spp)

    def pop_nowait(self):
        with self._lock:
            return self._q.popleft() if self._q else None

    def __len__(self):
        with self._lock:
            return len(self._q)


# ─── Frame producer ──────────────────────────────────────────────────────────
class FrameProducer:
    """Builds TM frames; idle-frames when queue empty."""
    def __init__(self, queue: SppQueue):
        self._q          = queue
        self._current    = b''
        self._cur_off    = 0
        self._mcfc       = 0
        self._vcfc       = 0
        self._n_data     = 0
        self._n_idle     = 0
        self._lock       = threading.Lock()

    @property
    def stats(self):
        with self._lock:
            return self._n_data, self._n_idle

    def _build_data_field(self):
        df = bytearray()
        fhp = FHP_NONE
        while len(df) < DATA_FIELD_LEN:
            if self._cur_off >= len(self._current):
                nxt = self._q.pop_nowait()
                if nxt is None:
                    break
                self._current = nxt
                self._cur_off = 0
                if fhp == FHP_NONE:
                    fhp = len(df)
            take = min(DATA_FIELD_LEN - len(df),
                       len(self._current) - self._cur_off)
            df.extend(self._current[self._cur_off:self._cur_off + take])
            self._cur_off += take
        if len(df) < DATA_FIELD_LEN:
            df.extend(b'\xAA' * (DATA_FIELD_LEN - len(df)))
        return bytes(df), fhp

    def next_frame(self) -> bytes:
        with self._lock:
            empty = (self._cur_off >= len(self._current)) and (len(self._q) == 0)
            if empty:
                df, fhp = b'\xAA' * DATA_FIELD_LEN, FHP_IDLE
                self._n_idle += 1
            else:
                df, fhp = self._build_data_field()
                self._n_data += 1
            frame = build_frame(df, self._mcfc, self._vcfc, fhp)
            self._mcfc = (self._mcfc + 1) & 0xFF
            self._vcfc = (self._vcfc + 1) & 0xFF
            return frame


# ─── GR custom source: pulls frames from FrameProducer ───────────────────────
class FrameSource(gr.sync_block):
    """
    Continuous uint8 stream source. Pulls frames from FrameProducer on demand
    and streams their bytes out. Never starves Pluto DMA: idle frames fill
    any silence.
    """
    def __init__(self, producer: FrameProducer):
        gr.sync_block.__init__(
            self, name="ccsds_frame_source",
            in_sig=None, out_sig=[np.uint8])
        self._producer = producer
        self._current  = b''
        self._pos      = 0

    def work(self, input_items, output_items):
        out = output_items[0]
        n   = len(out)
        w   = 0
        while w < n:
            if self._pos >= len(self._current):
                self._current = self._producer.next_frame()
                self._pos = 0
            take = min(n - w, len(self._current) - self._pos)
            out[w:w+take] = np.frombuffer(
                self._current[self._pos:self._pos+take], dtype=np.uint8)
            self._pos += take
            w += take
        return n


# ─── TX flowgraph: FrameSource -> GMSK mod -> PlutoSDR sink ──────────────────
class TxFlowgraph(gr.top_block):
    def __init__(self, producer: FrameProducer):
        gr.top_block.__init__(self, "TX Flowgraph", catch_exceptions=True)
        self.src = FrameSource(producer)
        self.mod = digital.gmsk_mod(
            samples_per_symbol=SPS, bt=BT,
            verbose=False, log=False, do_unpack=True)
        uri = iio.get_pluto_uri()
        self.sink = iio.fmcomms2_sink_fc32(uri, [True, True], 32768, False)
        self.sink.set_len_tag_key('')
        self.sink.set_frequency(CENTER_FREQ)
        self.sink.set_samplerate(SAMP_RATE)
        self.sink.set_bandwidth(SAMP_RATE)
        self.sink.set_attenuation(0, TX_ATTENUATION)
        self.sink.set_filter_params('Auto', '', 0, 0)
        self.connect(self.src, self.mod, self.sink)


# ─── Ref-side TCP server ─────────────────────────────────────────────────────
def handle_ref(conn: socket.socket, addr, queue: SppQueue,
               stop_event: threading.Event):
    print(f"[TX] Ref connected from {addr}")
    conn.settimeout(0.1)

    parser    = FixedSizeParser()
    bytes_in  = 0
    spp_count = 0

    try:
        while not stop_event.is_set():
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


# ─── Main ────────────────────────────────────────────────────────────────────
def main():
    stop_event = threading.Event()
    signal.signal(signal.SIGINT,  lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    print("=== bridge_tx.py — Chunk 5 (patched): TM over GMSK/PlutoSDR ===")
    print(f"    Frequency:  {CENTER_FREQ/1e6:.1f} MHz")
    print(f"    Modulation: GMSK (BT={BT}, SPS={SPS}, "
          f"symbol rate={SAMP_RATE/SPS/1000:.0f} ksym/s)")
    print(f"    OTA frame:  ASM(4) + TF({FRAME_LEN}) = {OTA_LEN}B")

    queue    = SppQueue()
    producer = FrameProducer(queue)

    print("[TX] Starting internal GNU Radio flowgraph...")
    tb = TxFlowgraph(producer)
    tb.start()
    print("[TX] PlutoSDR streaming.")

    def stats_loop():
        t0 = time.time()
        while not stop_event.is_set():
            stop_event.wait(timeout=5.0)
            data_n, idle_n = producer.stats
            print(f"[TX] [{time.time()-t0:6.0f}s] data_frames={data_n} "
                  f"idle_frames={idle_n} queue_depth={len(queue)}")
    threading.Thread(target=stats_loop, daemon=True).start()

    serve_ref(queue, stop_event)

    print("[TX] Stopping flowgraph...")
    tb.stop(); tb.wait()
    print("[TX] Stopped.")


if __name__ == '__main__':
    main()