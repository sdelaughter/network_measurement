import json
import subprocess
import sys
import time

learning_count = 20
learning_interval = 0.101
offset_correction = -0.0002 # Send slightly early to account for packet generation time

def learn_offset(dest):
    bashCommand = f"sudo ../pping/pping {dest} -c {learning_count} -i {learning_interval} -t 1 -j"
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE)
    process.wait()
    output, error = process.communicate()
    data=json.loads(output)

    # min_rtt = None
    # min_ts = None
    # for packet in data:
    #     if min_ts is None:
    #         min_rtt = packet['rtt']
    #         min_ts = packet['timestamp']
    #     elif packet['rtt'] < min_rtt:
    #         min_rtt = packet['rtt']
    #         min_ts = packet['timestamp']

    rtt_sum = 0
    min_rtt = float("inf")
    max_rtt = float("-inf")
    min_rtt_timestamp = None

    for entry in data:
        rtt = entry["rtt"]
        timestamp = entry["timestamp"]

        rtt_sum += rtt

        if rtt < min_rtt:
            min_rtt = rtt
            min_rtt_timestamp = timestamp

        if rtt > max_rtt:
            max_rtt = rtt

    mean_rtt = rtt_sum / len(data)

    print(f"min/mean/max RTTs: {min_rtt} / {mean_rtt:.2f} / {max_rtt} ms")
    print(f"Min RTT Timestamp: {min_rtt_timestamp}")
    return min_rtt_timestamp

def main():
    dest = sys.argv[1]
    print("Learning...")
    offset = learn_offset(dest) + offset_correction
    print("Probing...")
    bashCommand = f"sudo ../pping/pping {dest} -o {offset}"
    if len(sys.argv) > 2:
        bashCommand += " " + " ".join(sys.argv[2:])
    process = subprocess.Popen(bashCommand.split(), stdout=subprocess.PIPE, text=True)
    process.wait()
    output, error = process.communicate()
    print(output)

if __name__ == "__main__":
    main()