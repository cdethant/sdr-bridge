#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#
# SPDX-License-Identifier: GPL-3.0
#
# GNU Radio Python Flow Graph
# Title: Not titled yet
# Author: ethant
# GNU Radio version: 3.10.12.0

from PyQt5 import Qt
from gnuradio import qtgui
from PyQt5.QtCore import QObject, pyqtSlot
from gnuradio import blocks
import numpy
from gnuradio import digital
from gnuradio import gr
from gnuradio.filter import firdes
from gnuradio.fft import window
import sys
import signal
from PyQt5 import Qt
from argparse import ArgumentParser
from gnuradio.eng_arg import eng_float, intx
from gnuradio import eng_notation
from gnuradio import iio
import threading



class TX_test(gr.top_block, Qt.QWidget):

    def __init__(self):
        gr.top_block.__init__(self, "Not titled yet", catch_exceptions=True)
        Qt.QWidget.__init__(self)
        self.setWindowTitle("Not titled yet")
        qtgui.util.check_set_qss()
        try:
            self.setWindowIcon(Qt.QIcon.fromTheme('gnuradio-grc'))
        except BaseException as exc:
            print(f"Qt GUI: Could not set Icon: {str(exc)}", file=sys.stderr)
        self.top_scroll_layout = Qt.QVBoxLayout()
        self.setLayout(self.top_scroll_layout)
        self.top_scroll = Qt.QScrollArea()
        self.top_scroll.setFrameStyle(Qt.QFrame.NoFrame)
        self.top_scroll_layout.addWidget(self.top_scroll)
        self.top_scroll.setWidgetResizable(True)
        self.top_widget = Qt.QWidget()
        self.top_scroll.setWidget(self.top_widget)
        self.top_layout = Qt.QVBoxLayout(self.top_widget)
        self.top_grid_layout = Qt.QGridLayout()
        self.top_layout.addLayout(self.top_grid_layout)

        self.settings = Qt.QSettings("gnuradio/flowgraphs", "TX_test")

        try:
            geometry = self.settings.value("geometry")
            if geometry:
                self.restoreGeometry(geometry)
        except BaseException as exc:
            print(f"Qt GUI: Could not restore geometry: {str(exc)}", file=sys.stderr)
        self.flowgraph_started = threading.Event()

        ##################################################
        # Variables
        ##################################################
        self.source_select = source_select = 0
        self.samp_rate = samp_rate = 2000000

        ##################################################
        # Blocks
        ##################################################

        # Create the options list
        self._source_select_options = [0, 1]
        # Create the labels list
        self._source_select_labels = ['0xAA Pattern', 'Random']
        # Create the combo box
        self._source_select_tool_bar = Qt.QToolBar(self)
        self._source_select_tool_bar.addWidget(Qt.QLabel("Select Source" + ": "))
        self._source_select_combo_box = Qt.QComboBox()
        self._source_select_tool_bar.addWidget(self._source_select_combo_box)
        for _label in self._source_select_labels: self._source_select_combo_box.addItem(_label)
        self._source_select_callback = lambda i: Qt.QMetaObject.invokeMethod(self._source_select_combo_box, "setCurrentIndex", Qt.Q_ARG("int", self._source_select_options.index(i)))
        self._source_select_callback(self.source_select)
        self._source_select_combo_box.currentIndexChanged.connect(
            lambda i: self.set_source_select(self._source_select_options[i]))
        # Create the radio buttons
        self.top_layout.addWidget(self._source_select_tool_bar)
        self.iio_pluto_sink_0 = iio.fmcomms2_sink_fc32('usb:3.9.5' if 'usb:3.9.5' else iio.get_pluto_uri(), [True, True], 32768, False)
        self.iio_pluto_sink_0.set_len_tag_key('')
        self.iio_pluto_sink_0.set_bandwidth(20000000)
        self.iio_pluto_sink_0.set_frequency(433000000)
        self.iio_pluto_sink_0.set_samplerate(samp_rate)
        self.iio_pluto_sink_0.set_attenuation(0, 20.0)
        self.iio_pluto_sink_0.set_filter_params('Auto', '', 0, 0)
        self.digital_gmsk_mod_0 = digital.gmsk_mod(
            samples_per_symbol=4,
            bt=0.35,
            verbose=False,
            log=False,
            do_unpack=True)
        self.blocks_vector_source_x_0 = blocks.vector_source_b([0xAA]*128, True, 1, [])
        self.blocks_stream_to_tagged_stream_0 = blocks.stream_to_tagged_stream(gr.sizeof_char, 1, 1024, "packet_len")
        self.blocks_selector_0 = blocks.selector(gr.sizeof_char*1,source_select,0)
        self.blocks_selector_0.set_enabled(True)
        self.analog_random_source_x_0 = blocks.vector_source_b(list(map(int, numpy.random.randint(0, 256, 1000))), True)


        ##################################################
        # Connections
        ##################################################
        self.connect((self.analog_random_source_x_0, 0), (self.blocks_selector_0, 1))
        self.connect((self.blocks_selector_0, 0), (self.blocks_stream_to_tagged_stream_0, 0))
        self.connect((self.blocks_stream_to_tagged_stream_0, 0), (self.digital_gmsk_mod_0, 0))
        self.connect((self.blocks_vector_source_x_0, 0), (self.blocks_selector_0, 0))
        self.connect((self.digital_gmsk_mod_0, 0), (self.iio_pluto_sink_0, 0))


    def closeEvent(self, event):
        self.settings = Qt.QSettings("gnuradio/flowgraphs", "TX_test")
        self.settings.setValue("geometry", self.saveGeometry())
        self.stop()
        self.wait()

        event.accept()

    def get_source_select(self):
        return self.source_select

    def set_source_select(self, source_select):
        self.source_select = source_select
        self._source_select_callback(self.source_select)
        self.blocks_selector_0.set_input_index(self.source_select)

    def get_samp_rate(self):
        return self.samp_rate

    def set_samp_rate(self, samp_rate):
        self.samp_rate = samp_rate
        self.iio_pluto_sink_0.set_samplerate(self.samp_rate)




def main(top_block_cls=TX_test, options=None):

    qapp = Qt.QApplication(sys.argv)

    tb = top_block_cls()

    tb.start()
    tb.flowgraph_started.set()

    tb.show()

    def sig_handler(sig=None, frame=None):
        tb.stop()
        tb.wait()

        Qt.QApplication.quit()

    signal.signal(signal.SIGINT, sig_handler)
    signal.signal(signal.SIGTERM, sig_handler)

    timer = Qt.QTimer()
    timer.start(500)
    timer.timeout.connect(lambda: None)

    qapp.exec_()

if __name__ == '__main__':
    main()
