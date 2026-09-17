# network_measurement
Network Measurement Tools

## Setup
Run `setup_venv.sh` to create a python virtual environment with dependencies necessary for plotting in Jupyter notebooks.

## List of Tools
- [hopping (Hop-by-hop Ping)](#hopping)
- [kdelay (Kernel Component Delay)](#kdelay)
- [pping (Poisson Ping)](#pping)
- [udping (UDP Ping)](#udping)

### hopping
Similar to `traceroute`, but with a few advantages:
  - More accurate per-hop latency measurements by sending pings with normal TTLs after initial probe packets
  - Pass-through of all command-line arguments supported by `ping`
  - JSON formatted output for easier analysis

Also inscludes a `plot.ipynb` notebook for plotting measurements in various ways.

### kdelay
Provides various component measurements of directional latency between a client and server over UDP.  Includes kernel timestamps to distinguish time spent in queues and on the network.  Supports hardware timestamps where possible.

Similar to the [TWAMP](https://datatracker.ietf.org/doc/html/rfc5357) protocol, but provides no authentication or encryption.

Also includes a `plot.ipynb` notebook for computing and plotting various delays from the data.

### pping
Similar to `ping`, but sends packets at more precise, fine-grained intervals, at intervals sampled from Poisson or Uniform distributions, and provides significantly higher maximum packet rates when per-packet logging is enabled.

Outputs in the same format as `ping` by default to facilitate existing parsers (prefacing each line with a timestamp as would `ping -D`), but also supports JSON output.

### udping
A simpler version of `kdelay`, without kernel timestamps. Outputs the following per measurement:
`sequence, timestamp, rtt, send_delay, recv_delay`
ddi
Also includes a `plot.ipynb` notebook for plotting RTT and one-way delays
