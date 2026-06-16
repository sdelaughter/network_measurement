# Hopping
# Author: Sam DeLaughter
# Last Modified: 2026-05-28

# Similar to traceroute, but sends reguar pings to each hop after TTL probe for more accurate RTTs, and provides JSON output

# Positional Arguments:
# 1. Destination IP address
# 2. Number of pings to send to each hop (not including the initial TTL probe)
# 3. Any extra arguments to be passed through to the actual ping commands

# Example:
#    `python3 hopping.py 1.1.1.1 3 -W 0.5`
# This will find each address on the path to the destination IP of 1.1.1.1
# It will send 3 pings to each hop
# Each ping will time out after 0.5s because of the -W argument

import json
import subprocess
import sys
import time

json_indent = 4
ttl_max = 64 # Maximum TTL
ttl_increment = 1 # Amount to increment TTL

def ping_probe(dest, ttl=64, extra=None):
    if extra == None:
        extra = []

    probe_start = time.monotonic_ns()
    probe = subprocess.run(["ping", dest, "-c", "1", "-t", str(ttl)] + extra, capture_output=True)
    probe_end = time.monotonic_ns()
    probe_duration = (probe_end - probe_start) / 1000000.0
    probe_status = int(probe.returncode)
    probe_output = probe.stdout.decode()
    if probe_status == 0:
        ip = dest
    elif "From " in probe_output:
        ip = probe_output.split("From ")[1].split(" ")[0]
    else:
        ip = None

    return {
        "status": probe_status,
        "duration": probe_duration,
        "IP": ip
    }

def ping_hop(dest, count=1, extra=None):
    if extra == None:
        extra = []

    ping_output = subprocess.run(["ping", dest, "-c", str(count), "-D"] + extra, capture_output=True)
    output = ping_output.stdout.decode()
    parsed = output.split("\n")
    ping_list = []
    for line in parsed:
        line = line.strip()
        if not "icmp_seq=" in line:
            continue
        timestamp = float(line.split(" ")[0].strip('[]'))
        line = line.split("icmp_seq=")[1]
        seq = int(line.split(" ")[0])
        rtt = float(line.split("time=")[1].split(" ")[0])
        ping_list.append({
            "icmp_seq": seq,
            "RTT": rtt,
            "time": timestamp
        })
    return ping_list

def hopping(dest, count=1, extra=None):
    if extra == None:
        extra = []

    ttl = 0
    data = []
    prev_rtt = 0
    while True:
        ttl += 1
        if ttl > ttl_max:
            break
        
        data_row = {"TTL": ttl}
        probe = ping_probe(dest, ttl, extra)
        data_row["IP"] = probe["IP"]
        data_row["probe_status"] = probe["status"]
        data_row["probe_duration"] = probe["duration"]

        ping_list = []
        if probe["IP"] != None:
            ping_list = ping_hop(probe["IP"], count, extra)
        data_row["pings"] = ping_list

        if len(ping_list) > 0:
            rtt_list = [i["RTT"] for i in ping_list]
            mean_rtt = sum(rtt_list) / len(rtt_list)
            min_rtt = min(rtt_list)
            max_rtt = max(rtt_list)
            hop_diff = mean_rtt - prev_rtt
            prev_rtt = mean_rtt
        else:
            mean_rtt = None
            min_rtt = None
            max_rtt = None
            hop_diff = None

        data_row["mean"] = mean_rtt
        data_row["max"] = max_rtt
        data_row["min"] = min_rtt
        data_row["hop_diff"] = hop_diff

        data.append(data_row)

        if probe["status"] == 0:    
            break
    
    return data

def main():
    dest = sys.argv[1]
    count = sys.argv[2]
    extra = sys.argv[3:]

    data = hopping(dest, count, extra)
    print(json.dumps(data, indent=json_indent))

if __name__ == "__main__":
    main()
