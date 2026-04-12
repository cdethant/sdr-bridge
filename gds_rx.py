#!/usr/bin/env python3
"""
SDR RX Bridge: RTL-SDR → GMSK demod → TCP client to GDS

Receives RF packets, extracts payloads, forwards to GDS.

Usage:
  # Start GDS first:
  fprime-gds -n -g html --gui-addr 0.0.0.0 \
    --ip-address 0.0.0.0 \
    --dictionary ./dict/RefTopologyDictionary.json

  # Then start this bridge:
  ./sdr_bridge_rx.py
"""

import socket
import struct
import threading
import signal
import sys
import time
import numpy as np

from gnuradio import blocks, digital, gr
from gnuradio import filter as gr_filter
from gnuradio.fft import window
import osmosdr
import pmt

# Protocol constants — must match TX bridge
SYNC_WORD = 0xDEADBEEF
SYNC_BITS = ''.join(f'{b:08b}' for b in struct.pack('>I', SYNC_WORD))

# RF parameters — must match TX bridge
CENTER_FREQ = 433000000
SAMP_RATE = 2000000
SPS = 4
BT = 0.35
PPM_CORRECTION = 12
RF_GAIN = 30
IF_GAIN = 20
BB_GAIN = 20

# GDS TCP parameters
GDS_HOST = '127.0.0.1'   # where GDS is listening
GDS_PORT = 50000


class packet_forwarder(gr.sync_block):
    """
    Extracts packets from the correlated bit stream
    and forwards raw payloads to the GDS via TCP.
    """

    def __init__(self, tcp_send_fn):
        gr.sync_block.__init__(
            self, name="packet_forwarder",
            in_sig=[np.uint8], out_sig=None,
        )
        self._tcp_send = tcp_send_fn
        self._state = 'HUNT'
        self._pkt_len = 0
        self._bit_buf = []
        self._pkt_count = 0
        self._lock = threading.Lock()

    @property
    def pkt_count(self):
        with self._lock:
            return self._pkt_count

    def _bits_to_bytes(self, bits):
        out = bytearray()
        for i in range(0, len(bits) - 7, 8):
            val = 0
            for j in range(8):
                val = (val << 1) | (bits[i + j] & 1)
            out.append(val)
        return bytes(out)

    def work(self, input_items, output_items):
        inp = input_items[0]
        tags = self.get_tags_in_window(0, 0, len(inp))

        sync_offsets = set()
        for tag in tags:
            if pmt.symbol_to_string(tag.key) == 'sync':
                sync_offsets.add(int(tag.offset - self.nitems_read(0)))

        for i, bit in enumerate(inp):
            if i in sync_offsets:
                self._state = 'READ_LEN'
                self._bit_buf = []
                # Don't skip — include this bit

            if self._state == 'READ_LEN':
                self._bit_buf.append(int(bit))
                if len(self._bit_buf) == 16:
                    len_bytes = self._bits_to_bytes(self._bit_buf)
                    self._pkt_len = struct.unpack('>H', len_bytes)[0]
                    self._bit_buf = []
                    if self._pkt_len == 0 or self._pkt_len > 4096:
                        self._state = 'HUNT'
                    else:
                        self._state = 'READ_PAYLOAD'

            elif self._state == 'READ_PAYLOAD':
                self._bit_buf.append(int(bit))
                if len(self._bit_buf) == self._pkt_len * 8:
                    payload = self._bits_to_bytes(self._bit_buf)
                    with self._lock:
                        self._pkt_count += 1
                        count = self._pkt_count
                    print(f"[RF→GDS {count:>5d}] {self._pkt_len} bytes")
                    self._tcp_send(payload)
                    self._state = 'HUNT'
                    self._bit_buf = []

        return len(inp)


class rx_bridge(gr.top_block):

    def __init__(self, tcp_send_fn):
        gr.top_block.__init__(self, "RX Bridge", catch_exceptions=True)

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
            1.0, SAMP_RATE,
            symbol_rate * 0.6, symbol_rate * 0.2,
            window.WIN_HAMMING, 6.76,
        )
        self.lpf = gr_filter.fir_filter_ccf(1, lpf_taps)

        self.demod = digital.gmsk_demod(
            samples_per_symbol=SPS, verbose=False, log=False,
        )

        self.correlator = digital.correlate_access_code_tag_bb(
            SYNC_BITS, 2, 'sync',
        )

        self.forwarder = packet_forwarder(tcp_send_fn)

        self.connect(self.src, self.lpf, self.demod,
                     self.correlator, self.forwarder)


class gds_connection:
    """Manages TCP connection to GDS with auto-reconnect."""

    def __init__(self, host, port):
        self._host = host
        self._port = port
        self._sock = None
        self._lock = threading.Lock()

    def connect(self):
        while True:
            try:
                self._sock = socket.socket(
                    socket.AF_INET, socket.SOCK_STREAM
                )
                self._sock.connect((self._host, self._port))
                print(f"  Connected to GDS at {self._host}:{self._port}")
                return
            except ConnectionRefusedError:
                print(f"  GDS not ready at {self._host}:{self._port}"
                      f", retrying in 2s...")
                time.sleep(2)

    def send(self, data: bytes):
        with self._lock:
            if self._sock is None:
                return
            try:
                self._sock.sendall(data)
            except (BrokenPipeError, ConnectionResetError):
                print("  GDS connection lost, reconnecting...")
                self._sock.close()
                self.connect()
                try:
                    self._sock.sendall(data)
                except Exception:
                    pass

    def close(self):
        with self._lock:
            if self._sock:
                self._sock.close()
                self._sock = None


def main():
    gds = gds_connection(GDS_HOST, GDS_PORT)

    stop_event = threading.Event()

    def sig_handler(sig=None, frame=None):
        stop_event.set()

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    print("=== SDR RX Bridge ===")
    print(f"  Frequency : {CENTER_FREQ/1e6:.1f} MHz")
    print(f"  Modulation: GMSK (BT={BT}, sps={SPS})")
    print(f"  GDS target: {GDS_HOST}:{GDS_PORT}")

    gds.connect()

    bridge = rx_bridge(gds.send)
    bridge.start()

    print("\n  Bridge running. Ctrl+C to stop.\n")

    t0 = time.time()
    while not stop_event.is_set():
        try:
            stop_event.wait(timeout=5.0)
            elapsed = time.time() - t0
            count = bridge.forwarder.pkt_count
            print(f"  [{elapsed:6.0f}s] forwarded: {count} packets")
        except KeyboardInterrupt:
            break

    bridge.stop()
    bridge.wait()
    gds.close()
    print(f"\nStopped. Total forwarded: {bridge.forwarder.pkt_count}")


if __name__ == '__main__':
    main()
