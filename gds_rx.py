#!/usr/bin/env python3
"""
bridge_rx.py — Chunk 4: receive ASM-delimited TM frames, reassemble SPPs.

  - Listens on :52000 for bridge_tx.
  - Scans the byte stream for the ASM (0x1ACFFC1D).
  - For each detected ASM, reads the following 256-byte TM frame.
  - Validates CRC-16 FECF.
  - Tracks VCFC for drop detection.
  - Uses FHP to find SPP starts; reassembles SPPs that span frames.
  - Forwards complete 1024-byte SPPs to the GDS.
"""
import signal
import socket
import struct
import threading
import time


# ─── Config ──────────────────────────────────────────────────────────────────
TX_LISTEN_HOST = '0.0.0.0'
TX_LISTEN_PORT = 52000

GDS_HOST       = '127.0.0.1'
GDS_PORT       = 50000

RECV_BUF       = 4096

# F´ SPP parameters
SPP_SIZE       = 1024
SPP_SYNC       = (0x04, 0x42)

# CCSDS TM frame parameters
ASM            = bytes([0x1A, 0xCF, 0xFC, 0x1D])
FRAME_LEN      = 256
HEADER_LEN     = 6
FECF_LEN       = 2
DATA_FIELD_LEN = FRAME_LEN - HEADER_LEN - FECF_LEN     # 248

FHP_NONE       = 0x7FF
FHP_IDLE       = 0x7FE


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
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((self._host, self._port))
                s.settimeout(0.1)
                with self._lock:
                    self._sock = s
                print(f"[RX] Connected to GDS at {self._host}:{self._port}")
                return
            except (ConnectionRefusedError, OSError) as e:
                print(f"[RX] GDS not ready ({e}), retrying in 2s")
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
            data = s.recv(RECV_BUF)
            if not data:
                print("[RX] GDS closed its side.")
                with self._lock:
                    if self._sock is s:
                        self._sock.close()
                        self._sock = None
                return 0
            return len(data)
        except socket.timeout:
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
    """
    Scans an incoming TCP byte stream for the ASM, then reads the trailing
    256-byte TM frame. Yields validated TM frames as bytes.

    Resyncs automatically if bytes are dropped: on CRC failure, the search
    for the next ASM resumes from the byte after the current one.
    """
    def __init__(self):
        self._buf = bytearray()
        self.n_asm_found = 0
        self.n_crc_bad   = 0
        self.n_crc_good  = 0

    def feed(self, data: bytes):
        self._buf.extend(data)

    def pop_frames(self):
        """Generator: yield validated TM frames (256B each)."""
        while True:
            # Find next ASM
            idx = self._buf.find(ASM)
            if idx < 0:
                # Keep last 3 bytes in case ASM straddles next feed
                if len(self._buf) > 3:
                    del self._buf[:-3]
                return
            self.n_asm_found += 1

            # Wait for full frame
            if len(self._buf) < idx + len(ASM) + FRAME_LEN:
                # Trim leading junk before ASM so we don't search it again
                if idx > 0:
                    del self._buf[:idx]
                return

            frame_start = idx + len(ASM)
            frame = bytes(self._buf[frame_start:frame_start + FRAME_LEN])

            # Validate CRC
            hdr_and_df = frame[:HEADER_LEN + DATA_FIELD_LEN]
            fecf_recv  = struct.unpack(
                '>H', frame[HEADER_LEN + DATA_FIELD_LEN:])[0]
            if crc16_ccitt(hdr_and_df) != fecf_recv:
                self.n_crc_bad += 1
                # Resync: skip just past this ASM and search again
                del self._buf[:idx + 1]
                continue

            self.n_crc_good += 1
            # Consume up through end of this frame
            del self._buf[:frame_start + FRAME_LEN]
            yield frame


# ─── SPP reassembler ─────────────────────────────────────────────────────────
class SppReassembler:
    """
    Consumes data fields from TM frames. Uses FHP to locate the first SPP
    header in each frame; reassembles SPPs that span frames.

    State machine:
      - 'HUNT'     : waiting for first FHP-pointed SPP start
      - 'COLLECT'  : accumulating bytes until SPP_SIZE
    """
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
                # Lost frames; the in-progress SPP is unrecoverable
                self._state = 'HUNT'
                self._cur.clear()
        self._last_vcfc = vcfc

    def consume(self, data_field: bytes, fhp: int, vcfc: int):
        if fhp == FHP_IDLE:
            self._check_vcfc(vcfc)
            return
        self._check_vcfc(vcfc)

        idx = 0
        # If we're already collecting, append bytes up to the next SPP header
        # (or all of them if FHP_NONE).
        if self._state == 'COLLECT':
            end = fhp if fhp != FHP_NONE else DATA_FIELD_LEN
            end = min(end, DATA_FIELD_LEN)
            take = min(end - idx, SPP_SIZE - len(self._cur))
            self._cur.extend(data_field[idx:idx + take])
            idx += take
            if len(self._cur) == SPP_SIZE:
                self._emit()
            # If frame had more data than fit in this SPP and there's still
            # no FHP pointer, something is wrong; resync.
            if len(self._cur) > 0 and len(self._cur) < SPP_SIZE and fhp == FHP_NONE:
                return  # waiting for more frames

        # If FHP points into this frame, jump to it and start a new SPP
        if fhp != FHP_NONE:
            idx = fhp
            self._state = 'COLLECT'
            self._cur.clear()
            # Greedily extract all complete SPPs in this frame
            while idx < DATA_FIELD_LEN:
                take = min(DATA_FIELD_LEN - idx, SPP_SIZE - len(self._cur))
                self._cur.extend(data_field[idx:idx + take])
                idx += take
                if len(self._cur) == SPP_SIZE:
                    self._emit()
                else:
                    break  # need more frames to complete this SPP

    def _emit(self):
        spp = bytes(self._cur)
        self._cur.clear()
        self.n_spp += 1
        self._cb(spp)


# ─── Receive loop ────────────────────────────────────────────────────────────
def handle_tx(conn: socket.socket, addr, gds: GdsLink, stop_event: threading.Event):
    print(f"[RX] bridge_tx connected from {addr}")
    conn.settimeout(0.1)

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

    try:
        while not stop_event.is_set():
            try:
                data = conn.recv(RECV_BUF)
                if not data:
                    print("[RX] bridge_tx disconnected.")
                    break
                reader.feed(data)
                for frame in reader.pop_frames():
                    hdr = frame[:HEADER_LEN]
                    df  = frame[HEADER_LEN:HEADER_LEN + DATA_FIELD_LEN]
                    scid, vcid, mcfc, vcfc, fhp = parse_tm_header(hdr)
                    reassembler.consume(df, fhp, vcfc)
            except socket.timeout:
                pass
            except (ConnectionResetError, OSError) as e:
                print(f"[RX] bridge_tx recv failed: {e}")
                break

            gds.drain_heartbeat()
    finally:
        conn.close()
        print(f"[RX] Session ended. "
              f"frames_good={reader.n_crc_good} frames_bad={reader.n_crc_bad} "
              f"spps={reassembler.n_spp} drops_vcfc={reassembler.n_drop_vcfc} "
              f"bytes_to_gds={bytes_to_gds}")


def serve(gds: GdsLink, stop_event: threading.Event):
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((TX_LISTEN_HOST, TX_LISTEN_PORT))
    srv.listen(1)
    srv.settimeout(1.0)
    print(f"[RX] Listening for bridge_tx on {TX_LISTEN_HOST}:{TX_LISTEN_PORT}")

    try:
        while not stop_event.is_set():
            try:
                conn, addr = srv.accept()
            except socket.timeout:
                continue
            handle_tx(conn, addr, gds, stop_event)
    finally:
        srv.close()


def main():
    stop_event = threading.Event()
    signal.signal(signal.SIGINT,  lambda *_: stop_event.set())
    signal.signal(signal.SIGTERM, lambda *_: stop_event.set())

    print("=== bridge_rx.py — Chunk 4: TM frame deframing ===")
    print(f"    OTA frame:  ASM(4) + TF({FRAME_LEN}) = {len(ASM)+FRAME_LEN}B")
    print(f"    Data field: {DATA_FIELD_LEN}B per frame")
    print(f"    GDS:        {GDS_HOST}:{GDS_PORT}")

    gds = GdsLink(GDS_HOST, GDS_PORT)
    gds.connect(stop_event)
    if stop_event.is_set():
        return

    serve(gds, stop_event)
    gds.close()
    print("[RX] Stopped.")


if __name__ == '__main__':
    main()