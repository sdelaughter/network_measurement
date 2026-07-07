# Measure one-way delay components between devices
# Author: Samuel DeLaughter
# Last Modified: 2026-06-16

# OUTPUT
# Each line of output contains the following values
# sequence:             Sequence number
# ts:                   Time request is sent by the client (nanoseconds since epoch)
# rtt:                  Round Trip Time (ms)
# send:                 Outgoing one-way-delay (ms)
# recv:                 Incoming one-way-delay (ms)

# Note that one-way-delay metrics assume clock synchronization between client and server

# ARGUMENTS
# See the parse_args() function for all argument defintions, or run `python3 udping.py -h` to display full help info
# If running in server mode (with [-s/--server-mode]), any arguments other than [-A/--server-addr] and [-P/--server-port] will be ignored. 
# If running in client mode (the default), be sure to specify both the client address with [-a/--client-addr] and the server addresses with [-A/--server-addr].
# For the server to handle multiple simultaneous clients, each client must set a different client port with [-p/--client-port].

# EXAMPLE
# First start a server with:
#   python3 udping.py -A 10.0.0.1 -P 10000 -s
#
# Then, start a client with:
#   python3 udping.py -A 10.0.0.1 -P 10000 -a 10.0.0.2 -p 10001 -i 0.1 -c 5 -t 2 -x 64 -o udping.csv

import argparse
import signal
import socket
import struct
import sys
import time
from multiprocessing import Process, Event, Array

# Default parameters
DEFAULT_SERVER_ADDR = "127.0.0.1"
DEFAULT_SERVER_PORT = 10000
DEFAULT_CLIENT_ADDR = "127.0.0.1"
DEFAULT_CLIENT_PORT = 10001
DEFAULT_COUNT = 1
DEFAULT_TIMEOUT = 1
DEFAULT_INTERVAL = 1
DEFAULT_OUTFILE = None # If None, print to STDOUT
DEFAULT_PADDING = 0

# Other configuration
USE_NEW_TIMESTAMPS = True       # TODO: Determine based on system architecture
AMPLIFICATION_PREVENTION = True # Pad request packets to the minimum size of reply packets (padding option is in addition to this)
BUFF_SIZE = 1024                # TODO: compute actual space needed
ANC_BUFF_SIZE = 1024            # TODO: calculate with CMSG_SPACE()

# Packet struct formatting
BYTE_ORDER = "!" #Network order (big-endian)
STRUCT_FORMAT_REQUEST = "IQQ" # seq, client_port, ts_send
STRUCT_FORMAT_REPLY   = "IQQ" # seq, ts_send, ts_server
DATA_SIZE_REQUEST = len(struct.pack(BYTE_ORDER+STRUCT_FORMAT_REQUEST, *[0]*len(STRUCT_FORMAT_REQUEST)))    # 20 bytes
DATA_SIZE_REPLY   = len(struct.pack(BYTE_ORDER+STRUCT_FORMAT_REPLY, *[0]*len(STRUCT_FORMAT_REPLY)))        # 44 bytes

# Header names and value order for writing output
OUTPUT_ORDER = [
    "sequence",
    "ts",
    "rtt",
    "send",
    "recv"
]

def parse_args():
    parser = argparse.ArgumentParser(formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument("-s", "--server",       dest="server_mode", action='store_true',                            help="Run in server mode")
    parser.add_argument("-a", "--client-addr",  dest="client_addr", default=DEFAULT_CLIENT_ADDR,    type=str,       help="Client Address")
    parser.add_argument("-A", "--server-addr",  dest="server_addr", default=DEFAULT_SERVER_ADDR,    type=str,       help="Server Address")
    parser.add_argument("-p", "--client-port",  dest="client_port", default=DEFAULT_CLIENT_PORT,    type=int,       help="Client Port")
    parser.add_argument("-P", "--server-port",  dest="server_port", default=DEFAULT_SERVER_PORT,    type=int,       help="Server Port")
    parser.add_argument("-i", "--interval",     dest="interval",    default=DEFAULT_INTERVAL,       type=float,     help="Seconds between sending packets")
    parser.add_argument("-c", "--count",        dest="count",       default=DEFAULT_COUNT,          type=int,       help="Number of packets to send")
    parser.add_argument("-t", "--timeout",      dest="timeout",     default=DEFAULT_TIMEOUT,        type=float,     help="Seconds to wait after last packet sent")
    parser.add_argument("-o", "--outfile",      dest="outfile",     default=DEFAULT_OUTFILE,        type=str,       help="Output filepath.  Print to STDOUT if None")
    parser.add_argument("-x", "--padding",      dest="padding",     default=DEFAULT_PADDING,        type=int,       help="Padding bytes added")
    args = parser.parse_args()
    return args

class Server:
    def __init__(self,
        server_addr=DEFAULT_SERVER_ADDR,
        server_port=DEFAULT_SERVER_PORT,
        padding=DEFAULT_PADDING
    ):
        self.server_addr = server_addr
        self.server_port = server_port
        self.padding = padding

        self.running = False

        _ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_send = _ss

        _sr = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        _sr.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        _sr.bind((server_addr, server_port))
        self.sock_recv = _sr

        # Catch interrupt/terminate signals to exit cleanly
        signal.signal(signal.SIGINT, self.exit)
        signal.signal(signal.SIGTERM, self.exit)

    def reply(self):
        while self.running:
            data, ancdata, msg_flags, addr = self.sock_recv.recvmsg(BUFF_SIZE, ANC_BUFF_SIZE)
            ts_server = time.time_ns()
            pad_bytes = len(data) - DATA_SIZE_REQUEST
            i, port, ts_tx = struct.unpack(f"{BYTE_ORDER}{STRUCT_FORMAT_REQUEST}{pad_bytes}x", data)
            reply_data = struct.pack(f"{BYTE_ORDER}{STRUCT_FORMAT_REPLY}{self.padding}x", i, ts_tx, ts_server)
            self.sock_send.sendto(reply_data, (addr[0], port))

    def start(self):
        self.running = True
        self.reply()

    def stop(self):
        self.running = False
        self.sock_send.close()
        self.sock_recv.close()
    
    def exit(self, signal, frame):
        self.stop()
        sys.exit(0)


class Client:
    def __init__(self,
        client_addr = DEFAULT_CLIENT_ADDR,
        client_port = DEFAULT_CLIENT_PORT,
        server_addr = DEFAULT_SERVER_ADDR,
        server_port = DEFAULT_SERVER_PORT,
        timeout = DEFAULT_TIMEOUT,
        outfile = DEFAULT_OUTFILE,
        padding = DEFAULT_PADDING
    ):
        self.client_addr = client_addr
        self.client_port = client_port
        self.server_addr = server_addr
        self.server_port = server_port
        self.timeout = timeout

        self.outfile = outfile
        self.file_object = sys.stdout

        self.padding = padding
        
        self.stop_event_send = Event()
        self.stop_event_recv = Event()

        self.ring_buffer = None # Created on start()

        _sr = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        _sr.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        _sr.settimeout(self.timeout)
        _sr.bind((self.client_addr, self.client_port))
        self.sock_recv = _sr

        _ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock_send = _ss

        self.p_recv = None # Created on start()
        self.p_send = None # Created on start()

    
    def _write(self, line):
        self.file_object.write(line + "\n")

    def write_header(self):
        self._write(",".join(OUTPUT_ORDER))

    def write_measurement(self, m):
        self._write(",".join(str(m[k]) for k in OUTPUT_ORDER))

    def recv(self):
        self.write_header()
        while True:
            if self.stop_event_recv.is_set():
                break

            try:
                data, ancdata, msg_flags, addr = self.sock_recv.recvmsg(BUFF_SIZE, ANC_BUFF_SIZE)
            except:
                # Ignore socket timeouts so we can check for stop event without receiving any data
                continue

            ts_recv = time.time_ns()

            pad_bytes = len(data) - DATA_SIZE_REPLY
            seq, ts_send, ts_server = struct.unpack(f"{BYTE_ORDER}{STRUCT_FORMAT_REPLY}{pad_bytes}x", data)

            self.write_measurement({
                "sequence": seq,
                "ts": ts_send,
                "rtt": (ts_recv - ts_send)/1000000.0,
                "send": (ts_server - ts_send)/1000000.0,
                "recv": (ts_recv - ts_server)/1000000.0
            })

        self.stop_recv()
        self.close_file()

    def send(self, count=DEFAULT_COUNT, interval=DEFAULT_INTERVAL):
        i = 0
        while True:
            if self.stop_event_send.is_set():
                break

            i += 1
            if i > count:
                break

            ts_send = time.time_ns()
            pad_bytes = self.padding
            if AMPLIFICATION_PREVENTION:
                pad_bytes += max(0, DATA_SIZE_REPLY - DATA_SIZE_REQUEST)
            send_data = struct.pack(f"{BYTE_ORDER}{STRUCT_FORMAT_REQUEST}{pad_bytes}x", i, self.client_port, ts_send)
            self.sock_send.sendto(send_data, (self.server_addr, self.server_port))
            time.sleep(interval)

        time.sleep(self.timeout)
        self.stop()

    def start(self, count=DEFAULT_COUNT, interval=DEFAULT_INTERVAL):
        if not self.outfile is None:
            self.file_object = open(self.outfile, "a")
        self.stop_event_send = Event()
        self.stop_event_recv = Event()

        self.p_recv = Process(target=self.recv)
        self.p_send = Process(target=self.send, args=(
            count,
            interval
        ))

        self.p_recv.start()
        self.p_send.start()
        self.p_recv.join()
        self.p_send.join()


    def close_file(self):
        if not self.outfile is None:
            self.file_object.close()

    def stop_send(self):
        self.stop_event_send.set()
        self.sock_send.close()

    def stop_recv(self):
        self.stop_event_recv.set()
        self.sock_recv.close()

    def stop(self):
        self.stop_send()
        self.stop_recv()


if __name__ == '__main__':
    args = parse_args()

    if args.server_mode:
        server = Server(
            args.server_addr,
            args.server_port,
            args.padding
        )
        server.start()

    else:
        client = Client(
            args.client_addr,
            args.client_port,
            args.server_addr,
            args.server_port,
            args.timeout,
            args.outfile,
            args.padding
        )
        client.start(
            args.count,
            args.interval
        )
