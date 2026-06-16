# network_measurement
Network Measurement Tools

## List of Tools
### owd (One-Way-Delay)
Provides various component measurements of directional latency between a client and server, including time spent in queues and on the network.  Supports hardware timestamps where possible.

Also includes a `plot.ipynb` notebook for computing and plotting various delays from the data.

### hopping
Similar to traceroute, but with a few advantages:
  - More accurate per-hop latency measurements by sending pings with normal TTLs after initial probe packets
  - Pass-through of all command-line arguments supported by `ping`
  - JSON formatted output for easier analysis

Also inscludes a `plot.ipynb` notebook for plotting measurements in various ways.
