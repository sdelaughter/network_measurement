# network_measurement
Network Measurement Tools

## List of Tools
### owd (One-Way-Delay)
Provides various component measurements of directional latency between a client and server.  Output includes the following datapoints for each measurement:
  - Sequence number
  - Time request is scheduled by the client
  - Time request is sent by the client (software)
  - Time request is sent by the client (hardware transformed, if supported else same as above)
  - Time request is sent by the client (hardware raw, if supported else same as above)
  - Time request is received by the server
  - Time request is processed by the server
  - Time reply is sent by the server
  - Time reply is received by the client
  - Time reply is processed by the client

Also includes a `plot.ipynb` notebook for computing and plotting various delays from the data.

### hopping
Similar to traceroute, but with a few advantages:
  - More accurate per-hop latency measurements by sending pings with normal TTLs after initial probe packets
  - Pass-through of all command-line arguments supported by `ping`
  - JSON formatted output

Also inscludes a `plot.ipynb` notebook for plotting measurements in various ways.
