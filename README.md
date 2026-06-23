# network_measurement
Network Measurement Tools

## Setup
Run `setup_venv.sh` to create a python virtual environment with dependencies necessary for plotting in Jupyter notebooks.

## List of Tools

### hopping
Similar to traceroute, but with a few advantages:
  - More accurate per-hop latency measurements by sending pings with normal TTLs after initial probe packets
  - Pass-through of all command-line arguments supported by `ping`
  - JSON formatted output for easier analysis

Also inscludes a `plot.ipynb` notebook for plotting measurements in various ways.

### owd (One-Way-Delay)
A simplified version of the [TWAMP](https://datatracker.ietf.org/doc/html/rfc5357) protocol.

Provides various component measurements of directional latency between a client and server, including time spent in queues and on the network.  Supports hardware timestamps where possible.

Also includes a `plot.ipynb` notebook for computing and plotting various delays from the data.

### udping
A simpler version of `owd`, without kernel timestamps. Outputs the following per measurement:
`sequence, timestamp, rtt, send_delay, recv_delay`

Also includes a `plot.ipynb` notebook for plotting RTT and one-way delays
