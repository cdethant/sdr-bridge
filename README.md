# SDR bridge for F´ Python Ground Data System (GDS).
A bridge that routes the TCP interface of fprime & gds through SDR comms.

Before doing anything, set up the following:
- gnuradio
- plutosdr
- rtl-sdr

Because the bridge operates on two different hosts (1 for TX, 1 for RX), the respective branches hold the tx and rx side code.
