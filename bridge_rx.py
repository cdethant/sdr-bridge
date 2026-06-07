#!/usr/bin/env python3
"""
bridge_rx.py — Chunk 4: receive ASM-delimited TM frames, reassemble SPPs.
Integrated with GNU Radio flowgraph for RTL-SDR demodulation.
"""
import queue
import signal
import socket
import struct
import threading
import time

from gnuradio import blocks
from gnuradio import digital
from gnuradio import filter as gr_filter
from gnuradio import gr
from gnuradio.fft import window
import numpy as np
import osmosdr
import pmt

# ─── Config ──────────────────────────────────────────────────────────────────
GDS_HOST         = '127.0.0.1'
GDS_PORT         = 50000

RECV_BUF         = 4096

# F´ SPP parameters
SPP_SIZE         = 1024
SPP_SYNC         = (0x04, 0x42)

# CCSDS TM frame parameters
ASM              = bytes([0x1A, 0xCF, 0xFC, 0x1D])
FRAME_LEN        = 256
HEADER_LEN       = 6
FECF_LEN         = 2
DATA_FIELD_LEN   = FRAME_LEN - HEADER_LEN - FECF_LEN     # 248

FHP_NONE         = 0x7FF
FHP_IDLE         = 0x7FE

# RF parameters
CENTER_FREQ      = 433000000       # Hz
SAMP_RATE        = 2000000         # Hz
SPS              = 4               # samples per symbol
BT               = 0.35            # GMSK bandwidth-time product

# RTL-SDR specific
PPM_CORRECTION   = 12              # from rtl_test -p
RF_GAIN          = 30              # dB
IF_GAIN          = 20              # dB
BB_GAIN          = 20              # dB

SYNC_WORD        = 0x1ACFFC1D
SYNC_BITS        = ''.join(f'{b:08b}' for b in struct.pack('>I', SYNC_WORD))


# ─── GNU Radio Flowgraph ─────────────────────────────────────────────────────
class packet_sink(gr.sync_block):
    """
    Receives the tagged, unpacked bit stream from the correlator.
    On each 'sync' tag, reads 256 bytes of payload (2048 bits),
    and pushes ASM + 256 bytes to the thread-safe queue.
    """
    def __init__(self, out_q: queue.Queue):
        gr.sync_block.__init__(
            self,
            name="packet_sink",
            in_sig=[np.uint8],
            out_sig=None,
        )
        self._state = 'HUNT'
        self._bit_buf = []
        self._pkt_count = 0
        self._lock = threading.Lock()
        self.out_q = out_q

    @property
    def pkt_count(self):
        with self._lock:
            return self._pkt_count

    def work(self, input_items, output_items):
        inp = input_items[0]
        tags = self.get_tags_in_window(0, 0, len(inp))

        sync_offsets = []
        for tag in tags:
            if pmt.symbol_to_string(tag.key) == 'sync':
                sync_offsets.append(int(tag.offset - self.nitems_read(0)))
        sync_offsets.sort()

        idx = 0
        while idx < len(inp):
            if self._state == 'HUNT':
                next_sync = None
                for so in sync_offsets:
                    if so >= idx:
                        next_sync = so
                        break
                if next_sync is not None:
                    self._state = 'READ_PAYLOAD'
                    self._bit_buf = []
                    idx = next_sync
                else:
                    break
            elif self._state == 'READ_PAYLOAD':
                need = FRAME_LEN * 8 - len(self._bit_buf)
                
                next_sync = None
                for so in sync_offsets:
                    if so > idx and so < idx + need:
                        next_sync = so
                        break
                
                if next_sync is not None:
                    idx = next_sync
                    self._state = 'READ_PAYLOAD'
                    self._bit_buf = []
                    continue

                take = min(need, len(inp) - idx)
                self._bit_buf.extend(inp[idx : idx + take])
                idx += take

                if len(self._bit_buf) == FRAME_LEN * 8:
                    payload = np.packbits(np.array(self._bit_buf, dtype=np.uint8)).tobytes()
                    with self._lock:
                        self._pkt_count += 1
                    
                    frame = ASM + payload
                    self.out_q.put(frame)
                    
                    self._state = 'HUNT'
                    self._bit_buf = []

        return len(inp)


class RxFlowgraph(gr.top_block):
    def __init__(self, frame_queue: queue.Queue):
        gr.top_block.__init__(self, "RX Flowgraph", catch_exceptions=True)

        symbol_rate = SAMP_RATE / SPS

        self.src = osmosdr.source(args="numchan=1 rtl=0")
        self.src.set_sample_rate(SAMP_RATE)
        self.src.set_center_freq(CENTER_FREQ)
        self.src.set_freq_corr(PPM_CORRECTION)
        self.src.set_gain(RF_GAIN)
        self.src.set_if_gain(IF_GAIN)
        self.src.set_bb_gain(BB_GAIN)
        self.src.set_antenna('')
        self.src.set_dc_offset_mode(0)
        self.src.set_iq_balance_mode(0)

        lpf_taps = gr_filter.firdes.low_pass(
            1.0,                    # gain
            SAMP_RATE,              # sample rate
            symbol_rate * 0.6,      # cutoff freq
            symbol_rate * 0.2,      # transition width
            window.WIN_HAMMING,
            6.76,                   # beta
        )
        self.lpf = gr_filter.fir_filter_ccf(1, lpf_taps)

        self.demod = digital.gmsk_demod(
            samples_per_symbol=SPS,
            verbose=False,
            log=False,
        )

        self.correlator = digital.correlate_access_code_tag_bb(
            SYNC_BITS,
            2,          # threshold: max allowed bit errors
            'sync',     # tag key
        )

        self.pkt_sink = packet_sink(frame_queue)

        self.connect(self.src, self.lpf, self.demod)
        self.connect(self.demod, self.correlator, self.pkt_sink)


# ─── GDS link ────────────────────────────────────────────────────────────────
class GdsLink:
    def __init__(self, host: str, port: int):
        self._host = host
        self._port = port
        self._sock: socket.socket | None = None
        self._lock = threading.Lock()

    def connect(self, stop_event: threading.Event):
        while not stop_event.is_set():
            try:
                conn = socket.create_connection((self._host, self._port), timeout=1.0)
                conn.settimeout(0.1)
                with self._lock:
                    self._sock = conn
                print(f"[RX] Connected to GDS at {self._host}:{self._port}")
                return
            except (ConnectionRefusedError, OSError):
                print(f"[RX] Waiting for GDS at {self._host}:{self._port}...")
                time.sleep(2)
            except socket.timeout:
                continue
            except Exception as e:
                print(f"[RX] GDS connect error: {e}")
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
            print(f"[RX] Send to GDS failed: {e}")
            with self._lock:
                if self._sock is s:
                    self._sock.close()
                    self._sock = None
            return False

    def drain_heartbeat(self) -> int:
        with self._lock:
            s = self._sock
        if s is None:
            return 0
        try:
            s.settimeout(0.0)
            data = s.recv(RECV_BUF)
            s.settimeout(0.1)
            if not data:
                print("[RX] GDS closed its side.")
                with self._lock:
                    if self._sock is s:
                        self._sock.close()
                        self._sock = None
                return 0
            return len(data)
        except BlockingIOError:
            s.settimeout(0.1)
            return 0
        except socket.timeout:
            s.settimeout(0.1)
            return 0
        except (ConnectionResetError, OSError):
            with self._lock:
                if self._sock is s:
                    self._sock.close()
                    self._sock = None
            return 0

    def close(self):
        with self._lock:
            if self._sock:
                self._sock.close()
                self._sock = None


# ─── CCSDS framing primitives ────────────────────────────────────────────────
def crc16_ccitt(data: bytes, init: int = 0xFFFF) -> int:
    crc = init
    for b in data:
        crc ^= b << 8
        for _ in range(8):
            crc = ((crc << 1) ^ 0x1021) & 0xFFFF if crc & 0x8000 else (crc << 1) & 0xFFFF
    return crc


def parse_tm_header(hdr: bytes) -> tuple[int, int, int, int, int]:
    """Return (scid, vcid, mcfc, vcfc, fhp)."""
    w0, mcfc, vcfc, w3 = struct.unpack('>HBBH', hdr)
    scid = (w0 >> 4) & 0x3FF
    vcid = (w0 >> 1) & 0x7
    fhp  = w3 & 0x7FF
    return scid, vcid, mcfc, vcfc, fhp


# ─── ASM-delimited frame reader ──────────────────────────────────────────────
class FrameReader:
    def __init__(self):
        self._buf = bytearray()
        self.n_asm_found = 0
        self.n_crc_bad   = 0
        self.n_crc_good  = 0

    def feed(self, data: bytes):
        self._buf.extend(data)

    def pop_frames(self):
        while True:
            idx = self._buf.find(ASM)
            if idx < 0:
                if len(self._buf) > 3:
                    del self._buf[:-3]
                return
            self.n_asm_found += 1

            if len(self._buf) < idx + len(ASM) + FRAME_LEN:
                if idx > 0:
                    del self._buf[:idx]
                return

            frame_start = idx + len(ASM)
            frame = bytes(self._buf[frame_start:frame_start + FRAME_LEN])

            hdr_and_df = frame[:HEADER_LEN + DATA_FIELD_LEN]
            fecf_recv  = struct.unpack(
                '>H', frame[HEADER_LEN + DATA_FIELD_LEN:])[0]
            if crc16_ccitt(hdr_and_df) != fecf_recv:
                if self.n_crc_bad < 3:
                    print(f"[RX] CRC BAD dump: hdr={frame[:10].hex()}... expected_crc={fecf_recv:04x} calc={crc16_ccitt(hdr_and_df):04x}")
                self.n_crc_bad += 1
                del self._buf[:idx + 1]
                continue

            self.n_crc_good += 1
            del self._buf[:frame_start + FRAME_LEN]
            yield frame


# ─── SPP reassembler ─────────────────────────────────────────────────────────
class SppReassembler:
    def __init__(self, spp_cb):
        self._cb       = spp_cb
        self._state    = 'HUNT'
        self._cur      = bytearray()
        self._last_vcfc = None
        self.n_spp       = 0
        self.n_drop_vcfc = 0

    def _check_vcfc(self, vcfc: int):
        if self._last_vcfc is not None:
            gap = (vcfc - self._last_vcfc - 1) & 0xFF
            if gap:
                self.n_drop_vcfc += gap
                self._state = 'HUNT'
                self._cur.clear()
        self._last_vcfc = vcfc

    def consume(self, data_field: bytes, fhp: int, vcfc: int):
        if fhp == FHP_IDLE:
            self._check_vcfc(vcfc)
            return
        self._check_vcfc(vcfc)

        idx = 0
        if self._state == 'COLLECT':
            end = fhp if fhp != FHP_NONE else DATA_FIELD_LEN
            end = min(end, DATA_FIELD_LEN)
            take = min(end - idx, SPP_SIZE - len(self._cur))
            self._cur.extend(data_field[idx:idx + take])
            idx += take
            if len(self._cur) == SPP_SIZE:
                self._emit()
            if len(self._cur) > 0 and len(self._cur) < SPP_SIZE and fhp == FHP_NONE:
                return

        if fhp != FHP_NONE:
            idx = fhp
            self._state = 'COLLECT'
            self._cur.clear()
            while idx < DATA_FIELD_LEN:
                take = min(DATA_FIELD_LEN - idx, SPP_SIZE - len(self._cur))
                self._cur.extend(data_field[idx:idx + take])
                idx += take
                if len(self._cur) == SPP_SIZE:
                    self._emit()
                else:
                    break

    def _emit(self):
        spp = bytes(self._cur)
        self._cur.clear()
        self.n_spp += 1
        self._state = 'HUNT'
        self._cb(spp)


# ─── Receive loop ────────────────────────────────────────────────────────────
def process_frames(gds: GdsLink, frame_queue: queue.Queue, stop_event: threading.Event, pkt_sink: packet_sink):
    reader = FrameReader()
    bytes_to_gds = 0

    def on_spp(spp: bytes):
        nonlocal bytes_to_gds
        if spp[0] != SPP_SYNC[0] or spp[1] != SPP_SYNC[1]:
            print(f"[RX] WARN: reassembled SPP bad sync "
                  f"{spp[0]:02x} {spp[1]:02x}")
        if reassembler.n_spp <= 5 or reassembler.n_spp % 100 == 0:
            apid = ((spp[0] << 8) | spp[1]) & 0x07FF
            print(f"[RX] SPP #{reassembler.n_spp}: APID=0x{apid:03X} "
                  f"seq={spp[2]} len={len(spp)}B  "
                  f"frames(good={reader.n_crc_good} bad={reader.n_crc_bad}) "
                  f"drops_vcfc={reassembler.n_drop_vcfc}")
        if gds.send(spp):
            bytes_to_gds += len(spp)
        else:
            threading.Thread(target=gds.connect, args=(stop_event,),
                             daemon=True).start()

    reassembler = SppReassembler(on_spp)

    start_time = time.time()
    last_print = start_time

    while not stop_event.is_set():
        try:
            data = frame_queue.get(timeout=0.1)
            reader.feed(data)
            for frame in reader.pop_frames():
                hdr = frame[:HEADER_LEN]
                df  = frame[HEADER_LEN:HEADER_LEN + DATA_FIELD_LEN]
                scid, vcid, mcfc, vcfc, fhp = parse_tm_header(hdr)
                reassembler.consume(df, fhp, vcfc)
        except queue.Empty:
            pass

        gds.drain_heartbeat()
        
        now = time.time()
        if now - last_print >= 5.0:
            last_print = now
            print(f"[RX] [{(now - start_time):5.0f}s] "
                  f"frames_detected={pkt_sink.pkt_count} "
                  f"frames_good={reader.n_crc_good} "
                  f"frames_bad={reader.n_crc_bad} "
                  f"spps={reassembler.n_spp}")


    print(f"[RX] Session ended. "
          f"frames_good={reader.n_crc_good} frames_bad={reader.n_crc_bad} "
          f"spps={reassembler.n_spp} drops_vcfc={reassembler.n_drop_vcfc} "
          f"bytes_to_gds={bytes_to_gds}")


def main():
    stop_event = threading.Event()
    signal.signal(signal.SIGINT,  lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    print("=== bridge_rx.py — Integrated RX ===")
    print(f"    OTA frame:  ASM(4) + TF({FRAME_LEN}) = {len(ASM)+FRAME_LEN}B")
    print(f"    GDS:        {GDS_HOST}:{GDS_PORT}")

    gds = GdsLink(GDS_HOST, GDS_PORT)
    gds.connect(stop_event)
    if stop_event.is_set():
        return

    frame_queue = queue.Queue()

    print("[RX] Starting internal GNU Radio flowgraph...")
    tb = RxFlowgraph(frame_queue)
    tb.start()
    print("[RX] RTL-SDR listening.")

    process_frames(gds, frame_queue, stop_event, tb.pkt_sink)

    print("[RX] Stopping flowgraph...")
    tb.stop()
    tb.wait()
    
    gds.close()
    print("[RX] Stopped.")

if __name__ == '__main__':
    main()
