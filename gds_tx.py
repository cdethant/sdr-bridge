#!/usr/bin/env python3
"""
SDR TX Bridge: TCP server → GMSK → PlutoSDR

Ref connects here instead of directly to the GDS.
Bytes received on TCP are framed and transmitted over RF.

Usage:
  # Start this bridge first:
  ./sdr_bridge_tx.py

  # Then start Ref, pointing at this bridge:
  ./Ref -a 127.0.0.1 -p 50000
"""

import socket
import struct
import threading
import signal
import sys
import time

from gnuradio import blocks, digital, gr, pdu
from gnuradio import iio

# Protocol constants — must match RX bridge
SYNC_WORD = 0xDEADBEEF
PREAMBLE = bytes([0xAA] * 64)
INTER_PACKET_GAP = bytes([0x00] * 16)  # dead air between RF packets
SYNC_BYTES = struct.pack('>I', SYNC_WORD)

# RF parameters — must match RX bridge
CENTER_FREQ = 433000000
SAMP_RATE = 2000000
SPS = 4
BT = 0.35
TX_ATTENUATION = 10.0

# FPP framing parameters
FPP_START_WORD = b'\xA5\xA5\xA5\xA5'
FPP_HEADER_SIZE = 9  # start(4) + type(1) + size(4) — size field at offset 5

# TCP parameters
TCP_HOST = '0.0.0.0'
TCP_PORT = 50000


def make_packet(payload: bytes) -> bytes:
    length = struct.pack('>H', len(payload))
    return PREAMBLE + SYNC_BYTES + length + payload


class tx_bridge(gr.top_block):
    def __init__(self):
        gr.top_block.__init__(self, "TX Bridge", catch_exceptions=True)

        # Message-based source: we'll push PDUs into this
        self.pdu_src = blocks.pdu_to_tagged_stream(
                0, 'packet_len'
                )

        self.mod = digital.gmsk_mod(
            samples_per_symbol=SPS,
            bt=BT,
            verbose=False,
            log=False,
            do_unpack=True,
        )

        uri = iio.get_pluto_uri()
        self.sink = iio.fmcomms2_sink_fc32(
            uri, [True, True], 32768, False
        )
        self.sink.set_len_tag_key('')
        self.sink.set_frequency(CENTER_FREQ)
        self.sink.set_samplerate(SAMP_RATE)
        self.sink.set_bandwidth(SAMP_RATE)
        self.sink.set_attenuation(0, TX_ATTENUATION)
        self.sink.set_filter_params('Auto', '', 0, 0)

        self.connect(self.pdu_src, self.mod, self.sink)

    def send_packet(self, payload: bytes):
        """Frame and transmit a payload over RF."""
        import pmt
        packet = make_packet(payload)
        vec = pmt.init_u8vector(len(packet), list(packet))
        self.pdu_src.to_basic_block()._post(
            pmt.intern("pdus"),
            pmt.cons(pmt.PMT_NIL, vec)
        )


def tcp_server(bridge, stop_event):
    """Accept a TCP connection from Ref, read bytes, send over RF."""
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((TCP_HOST, TCP_PORT))
    srv.listen(1)
    srv.settimeout(2.0)

    print(f"  TCP listening on {TCP_HOST}:{TCP_PORT}")
    print(f"  Start Ref with: ./Ref -a 127.0.0.1 -p {TCP_PORT}")

    while not stop_event.is_set():
        try:
            conn, addr = srv.accept()
        except socket.timeout:
            continue

        print(f"  Ref connected from {addr}")
        conn.settimeout(0.1)
        buf = bytearray()
        pkt_count = 0

        while not stop_event.is_set():
            try:
                data = conn.recv(4096)
                if not data:
                    print("  Ref disconnected.")
                    break
                buf.extend(data)
            except socket.timeout:
                pass

            # Send complete chunks
            while len(buf) >= FPP_HEADER_SIZE:
                # Find FPP start word
                idx = buf.find(FPP_START_WORD)
                if idx < 0:
                    buf.clear()
                    break
                if idx > 0:
                    buf = buf[idx:]  # discard junk before start word

                if len(buf) < FPP_HEADER_SIZE:
                    break

                # FPP size field: big-endian u32 at offset 5
                frame_size = struct.unpack_from('>I', buf, 5)[0]
                total_len = 5 + 4 + frame_size

                if len(buf) < total_len:
                    break  # incomplete frame, wait for more

                frame = bytes(buf[:total_len])
                buf = buf[total_len:]
                bridge.send_packet(frame)
                pkt_count += 1
                time.sleep(0.002)  # 2ms inter-packet gap for RX to resync

        conn.close()
        print(f"  Sent {pkt_count} RF packets this session.")

    srv.close()


def main():
    bridge = tx_bridge()
    stop_event = threading.Event()

    def sig_handler(sig=None, frame=None):
        stop_event.set()

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    print("=== SDR TX Bridge ===")
    print(f"  Frequency : {CENTER_FREQ/1e6:.1f} MHz")
    print(f"  Modulation: GMSK (BT={BT}, sps={SPS})")

    bridge.start()
    tcp_thread = threading.Thread(
        target=tcp_server, args=(bridge, stop_event), daemon=True
    )
    tcp_thread.start()

    print("\n  Bridge running. Ctrl+C to stop.\n")

    while not stop_event.is_set():
        try:
            stop_event.wait(timeout=1.0)
        except KeyboardInterrupt:
            break

    bridge.stop()
    bridge.wait()
    print("\nStopped.")


if __name__ == '__main__':
    main()
