# SDR bridge for F´ Python Ground Data System (GDS).
A bridge that routes the TCP interface of fprime & gds through SDR comms.

Before doing anything, set up the following:
- gnuradio
- plutosdr
- rtl-sdr

Because the bridge operates on two different hosts (1 for TX, 1 for RX), the respective branches hold the tx and rx side code.

To run:
1. On the GDS side:

```
fprime-gds -n -g html --gui-addr 0.0.0.0 \
  --ip-address 0.0.0.0 \
  --dictionary ./dict/RefTopologyDictionary.json \
  --persistent-db
```

```python3 gds_rx.py```


2. On the fprime side:

```python3 gds_tx.py```

```sudo ./Ref/build-artifacts/Linux/Ref/bin/Ref -a 127.0.0.1 -p 50000``` (Note the ref points to a local ip because the SDR is local)
