#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# SPDX-License-Identifier: GPL-3.0
#
# GNU Radio Python Flow Graph
# Title: Not titled yet
# Author: ethant

# RX Flowgraph: RTL-SDR → GMSK demod → packet extraction
# Receives framed packets: [preamble][sync][length][payload]
# and prints decoded payloads to stdout.
# GNU Radio version: 3.10.12.0

from gnuradio import blocks
from gnuradio import digital
from gnuradio import filter as gr_filter
from gnuradio import gr
from gnuradio.fft import window
import numpy as np
import osmosdr
import pmt
import signal
import struct
import sys
import threading
import time
 
##################################################
# Protocol constants — must match TX
##################################################
SYNC_WORD = 0xDEADBEEF
PREAMBLE_LEN = 32
 
# Sync word as a bit string for the correlator
SYNC_BITS = ''.join(f'{b:08b}' for b in struct.pack('>I', SYNC_WORD))
 
##################################################
# RF parameters — must match TX
##################################################
CENTER_FREQ = 433000000       # Hz
SAMP_RATE = 2000000           # Hz
SPS = 4                       # samples per symbol
BT = 0.35                     # GMSK bandwidth-time product
 
# RTL-SDR specific
PPM_CORRECTION = 12           # from rtl_test -p
RF_GAIN = 30                  # dB
IF_GAIN = 20                  # dB
BB_GAIN = 20                  # dB
 
 
class packet_sink(gr.sync_block):
    """
    Receives the tagged, unpacked bit stream from the correlator.
    On each 'sync' tag (placed by correlate_access_code_tag):
      - reads the next 16 bits as big-endian payload length
      - reads that many bytes of payload
      - prints the decoded payload
    """
 
    def __init__(self):
        gr.sync_block.__init__(
            self,
            name="packet_sink",
            in_sig=[np.uint8],
            out_sig=None,
        )
        self._buffer = bytearray()
        self._state = 'HUNT'    # HUNT → GOT_SYNC → READ_LEN → READ_PAYLOAD
        self._pkt_len = 0
        self._bit_buf = []
        self._pkt_count = 0
        self._lock = threading.Lock()
 
    @property
    def pkt_count(self):
        with self._lock:
            return self._pkt_count
 
    def _bits_to_bytes(self, bits):
        """Pack a list of 0/1 ints into bytes, MSB first."""
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
 
        # Build a set of tag offsets (relative to this buffer)
        sync_offsets = set()
        for tag in tags:
            if pmt.symbol_to_string(tag.key) == 'sync':
                sync_offsets.add(int(tag.offset - self.nitems_read(0)))
 
        for i, bit in enumerate(inp):
            if i in sync_offsets:
                # Sync word found — start collecting length field
                self._state = 'READ_LEN'
                self._bit_buf = []
 
            if self._state == 'READ_LEN':
                self._bit_buf.append(int(bit))
                if len(self._bit_buf) == 16:  # 2 bytes for length
                    len_bytes = self._bits_to_bytes(self._bit_buf)
                    self._pkt_len = struct.unpack('>H', len_bytes)[0]
                    self._bit_buf = []
                    if self._pkt_len == 0 or self._pkt_len > 4096:
                        # Bogus length — probably a false sync
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
                    # Print decoded packet
                    try:
                        text = payload.decode('utf-8', errors='replace')
                    except Exception:
                        text = payload.hex()
                    print(f"[PKT {count:>4d}] "
                          f"len={self._pkt_len} "
                          f"hex={payload[:16].hex()}"
                          f"{'...' if len(payload) > 16 else ''} "
                          f"ascii=\"{text}\"")
                    self._state = 'HUNT'
                    self._bit_buf = []
 
            # else: HUNT — just consume bits until correlator fires
 
        return len(inp)
 
 
class headless_rx(gr.top_block):
 
    def __init__(self):
        gr.top_block.__init__(self, "Headless RX", catch_exceptions=True)
 
        symbol_rate = SAMP_RATE / SPS
 
        ##################################################
        # Source: RTL-SDR via osmocom
        ##################################################
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
 
        ##################################################
        # Channel filter
        ##################################################
        lpf_taps = gr_filter.firdes.low_pass(
            1.0,                    # gain
            SAMP_RATE,              # sample rate
            symbol_rate * 0.6,      # cutoff freq
            symbol_rate * 0.2,      # transition width
            window.WIN_HAMMING,
            6.76,                   # beta
        )
        self.lpf = gr_filter.fir_filter_ccf(
            1,                      # decimation
            lpf_taps,
        )
 
        ##################################################
        # Demodulator
        ##################################################
        self.demod = digital.gmsk_demod(
            samples_per_symbol=SPS,
            verbose=False,
            log=False,
        )
 
        ##################################################
        # Sync word correlator
        #
        # Searches the unpacked bit stream for SYNC_BITS.
        # When found, it tags the NEXT sample (first bit
        # after the sync word) with key='sync'.
        # Threshold = max bit errors allowed in the sync
        # word match. Start tight (2), loosen if needed.
        ##################################################
        self.correlator = digital.correlate_access_code_tag_bb(
            SYNC_BITS,
            2,          # threshold: max allowed bit errors
            'sync',     # tag key
        )
 
        ##################################################
        # Packet extractor
        ##################################################
        self.pkt_sink = packet_sink()
 
        ##################################################
        # Optional: also dump raw demod bits for debugging
        ##################################################
        self.file_sink = blocks.file_sink(
            gr.sizeof_char, '/tmp/rx_demod.bin', False
        )
        self.file_sink.set_unbuffered(False)
 
        ##################################################
        # Connections
        ##################################################
        #
        # src → lpf → demod → correlator → packet_sink
        #                  └→ file_sink (debug tap)
        #
        self.connect(self.src, self.lpf, self.demod)
        self.connect(self.demod, self.correlator, self.pkt_sink)
        self.connect(self.demod, self.file_sink)
 
 
def main():
    tb = headless_rx()
 
    stop_event = threading.Event()
 
    def sig_handler(sig=None, frame=None):
        stop_event.set()
 
    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)
 
    print("=== RX Flowgraph ===")
    print(f"  Frequency    : {CENTER_FREQ/1e6:.1f} MHz")
    print(f"  Sample rate   : {SAMP_RATE/1e6:.1f} Msps")
    print(f"  Symbol rate   : {SAMP_RATE/SPS/1e3:.1f} ksym/s")
    print(f"  Modulation    : GMSK (BT={BT}, sps={SPS})")
    print(f"  PPM correction: {PPM_CORRECTION}")
    print(f"  Gains         : RF={RF_GAIN} IF={IF_GAIN} BB={BB_GAIN} dB")
    print(f"  Sync word     : 0x{SYNC_WORD:08X} ({len(SYNC_BITS)} bits)")
    print(f"  Correlator thr: 2 bit errors")
    print(f"  Debug dump    : /tmp/rx_demod.bin")
    print()
    print("Listening. Ctrl+C to stop.")
    print()
 
    tb.start()
    t0 = time.time()
 
    while not stop_event.is_set():
        try:
            stop_event.wait(timeout=5.0)
            elapsed = time.time() - t0
            count = tb.pkt_sink.pkt_count
            print(f"  [{elapsed:6.0f}s] packets decoded: {count}")
        except KeyboardInterrupt:
            break
 
    tb.stop()
    tb.wait()
    print(f"\nStopped. Total packets: {tb.pkt_sink.pkt_count}")
 
 
if __name__ == '__main__':
    main()
