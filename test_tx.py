#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# SPDX-License-Identifier: GPL-3.0
#
# GNU Radio Python Flow Graph
# Title: Not titled yet
# Author: ethant
# TX Flowgraph: GMSK modulator → PlutoSDR
# Transmits framed packets: [preamble][sync][length][payload]
#
# GNU Radio version: 3.10.12.0
 
from gnuradio import blocks
from gnuradio import digital
from gnuradio import gr
from gnuradio import iio
import signal
import struct
import sys
import threading
 
##################################################
# Protocol constants — must match RX
##################################################
SYNC_WORD = 0xDEADBEEF
PREAMBLE_LEN = 32                       # bytes of 0xAA for timing recovery
PREAMBLE = bytes([0xAA] * PREAMBLE_LEN)
MSG = b"Testing RF"

# Sync word as a bit string for the correlator on RX side
SYNC_BITS = ''.join(f'{b:08b}' for b in struct.pack('>I', SYNC_WORD))
 
##################################################
# RF parameters
##################################################
CENTER_FREQ = 433000000       # Hz
SAMP_RATE = 2000000           # Hz
SPS = 4                       # samples per symbol
BT = 0.35                     # GMSK bandwidth-time product
TX_ATTENUATION = 10.0         # dB (lower = more power)
PLUTO_URI = ''                # empty string = auto-detect
 
 
def make_packet(payload: bytes) -> bytes:
    """Build a framed packet: preamble + sync + length(2B) + payload."""
    sync = struct.pack('>I', SYNC_WORD)
    length = struct.pack('>H', len(payload))
    return PREAMBLE + sync + length + payload
 
 
class headless_tx(gr.top_block):
 
    def __init__(self, payload: bytes = MSG):
        gr.top_block.__init__(self, "Headless TX", catch_exceptions=True)
 
        self.payload = payload
        self.packet = make_packet(payload)
 
        ##################################################
        # Source: repeating framed packet
        ##################################################
        self.src = blocks.vector_source_b(
            list(self.packet), True, 1, []
        )
 
        # Tag every packet-length boundary so the modulator
        # sees discrete packets (needed for burst-mode later)
        self.tagger = blocks.stream_to_tagged_stream(
            gr.sizeof_char, 1, len(self.packet), "packet_len"
        )
 
        ##################################################
        # Modulator
        ##################################################
        self.mod = digital.gmsk_mod(
            samples_per_symbol=SPS,
            bt=BT,
            verbose=False,
            log=False,
            do_unpack=True,
        )
 
        ##################################################
        # Sink: PlutoSDR
        ##################################################
        uri = PLUTO_URI if PLUTO_URI else iio.get_pluto_uri()
        self.sink = iio.fmcomms2_sink_fc32(
            uri,
            [True, True],          # single TX channel
            32768,                  # buffer size
            False,                  # cyclic
        )
        self.sink.set_len_tag_key('')
        self.sink.set_frequency(CENTER_FREQ)
        self.sink.set_samplerate(SAMP_RATE)
        self.sink.set_bandwidth(SAMP_RATE)  # match signal BW
        self.sink.set_attenuation(0, TX_ATTENUATION)
        self.sink.set_filter_params('Auto', '', 0, 0)
 
        ##################################################
        # Connections
        ##################################################
        self.connect(self.src, self.tagger, self.mod, self.sink)
 
 
def main():
    if len(sys.argv) > 1:
        payload = ' '.join(sys.argv[1:]).encode()
    else:
        payload = MSG
 
    tb = headless_tx(payload)
 
    stop_event = threading.Event()
 
    def sig_handler(sig=None, frame=None):
        stop_event.set()
 
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
 
    pkt = tb.packet
    print("=== TX Flowgraph ===")
    print(f"  Frequency   : {CENTER_FREQ/1e6:.1f} MHz")
    print(f"  Sample rate  : {SAMP_RATE/1e6:.1f} Msps")
    print(f"  Symbol rate  : {SAMP_RATE/SPS/1e3:.1f} ksym/s")
    print(f"  Modulation   : GMSK (BT={BT}, sps={SPS})")
    print(f"  TX atten     : {TX_ATTENUATION} dB")
    print(f"  Payload      : {tb.payload}")
    print(f"  Packet       : {len(pkt)} bytes "
          f"({PREAMBLE_LEN} preamble + 4 sync + 2 len + {len(tb.payload)} data)")
    print(f"  Sync bits    : {SYNC_BITS}")
    print()
    print("Transmitting. Ctrl+C to stop.")
 
    tb.start()
 
    while not stop_event.is_set():
        try:
            stop_event.wait(timeout=1.0)
        except KeyboardInterrupt:
            break
 
    tb.stop()
    tb.wait()
    print("\nStopped.")
 
 
if __name__ == '__main__':
    main()
