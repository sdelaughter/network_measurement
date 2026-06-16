# Measure one-way delay components between devices
# Author: Samuel DeLaughter
# Last Modified: 2026-06-16

# OUTPUT
# Each line of output contains the following values
# sequence:             Sequence number
# client_schedule:      Time packet is scheduled by the client
# client_send:          Time packet is actually sent by the client kernel (software)
# client_send_hw:       Time packet is actually sent by the client kernel (hardware_transformed), or =client_send if unsupported
# client_send_hw_raw:   Time packet is actually sent by the client kernel (hardware_raw), or =client_send_hw if unsupported
# server_recv:          Time packet is received by the server's kernel
# server_process:       Time packet is processed by the server's program
# reply_recv:           Time reply is received by the client's kernel
# reply_process:        Time reply is processed by the client's program

# All timestamps are given in nanoseconds since the epoch
# The server_recv and server_process timestamps are based on the server's clock, while the rest are based on the client's clock

# ARGUMENTS
# See the parse_args() function for all argument defintions, or run `python3 owd.py -h` to display full help info
# If running in server mode (with [-s/--server-mode]), any arguments other than [-A/--server-addr] and [-P/--server-port] will be ignored. 
# If running in client mode (the default), be sure to specify both the client address with [-a/--client-addr] and the server addresses with [-A/--server-addr].
# For the server to handle multiple simultaneous clients, each client must set a different client port with [-p/--client-port].

# EXAMPLE
# First start a server with:
#   python3 owd.py -A 10.0.0.1 -P 10000 -s
#
# Then, start a client with:
#   python3 owd.py -A 10.0.0.1 -P 10000 -a 10.0.0.2 -p 10001 -i 0.1 -c 5 -t 2 -o owd.csv

import argparse
import ctypes
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

# Packet size constants, to determine padding amounts
DATA_SIZE_REQUEST = 16  # IIQ
DATA_SIZE_REPLY = 28    # IQQQ

# Socket constants
SO_TIMESTAMPNS = 35
SO_TIMESTAMPING = 37
SOF_TIMESTAMPING_TX_HARDWARE = (1<<0)
SOF_TIMESTAMPING_TX_SOFTWARE = (1<<1)
SOF_TIMESTAMPING_SOFTWARE = (1<<4)
BUFF_SIZE = 1024        # TODO: compute actual space needed
ANC_BUFF_SIZE = 1024    # TODO: calculate with CMSG_SPACE()

# Ring buffer constants
RING_SIZE = 1024     # Max outstanding packets
RING_SLOT_SEQ = 0
RING_SLOT_TS = 1
RING_SLOT_TS2 = 2
RING_SLOT_TS3 = 3
RING_SLOTS = 4      # Number of values stored per packet

OUTPUT_ORDER = [
    "sequence",
    "client_schedule",
    "client_send",
    "client_send_hw",
    "client_send_hw_raw",
    "server_recv",
    "server_process",
    "reply_recv",
    "reply_process"
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

def ring_create(size=RING_SIZE):
    return Array(ctypes.c_int64, [-1]*RING_SLOTS*size, lock=False)

def ring_put(ring, seq, ts, ts2, ts3, size=RING_SIZE):
    i = (seq % size) * RING_SLOTS
    ring[i+RING_SLOT_SEQ] = seq
    ring[i+RING_SLOT_TS] = ts
    ring[i+RING_SLOT_TS2] = ts2
    ring[i+RING_SLOT_TS3] = ts3

def ring_pop(ring, seq, size=RING_SIZE):
    i = (seq % size) * RING_SLOTS
    if ring[i+RING_SLOT_SEQ] == seq:
        ts = ring[i+RING_SLOT_TS]
        ts2 = ring[i+RING_SLOT_TS2]
        ts3 = ring[i+RING_SLOT_TS3]
        ring[i+RING_SLOT_SEQ] = -1
        return [ts, ts2, ts3]
    return None

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
        _sr.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMPNS, 1)
        _sr.bind((server_addr, server_port))
        self.sock_recv = _sr

        # Catch interrupt/terminate signals to exit cleanly
        signal.signal(signal.SIGINT, self.exit)
        signal.signal(signal.SIGTERM, self.exit)

    def start(self):
        self.running = True
        while self.running:
            data, ancdata, msg_flags, addr = self.sock_recv.recvmsg(BUFF_SIZE, ANC_BUFF_SIZE)
            rx_ns = time.time_ns()
            rx_ns_kernel = rx_ns
            for level, ctype, cdata in ancdata:
                if (
                    level == socket.SOL_SOCKET and
                    ctype == SO_TIMESTAMPNS
                ):
                    sec, nsec = struct.unpack("qq", cdata)
                    rx_ns_kernel = sec * 1000000000 + nsec

            pad_bytes = len(data) - DATA_SIZE_REQUEST
            i, port, tx_ns = struct.unpack(f"!IIQ{pad_bytes}x", data)
            reply_data = struct.pack(f"!IQQQ{self.padding}x", i, tx_ns, rx_ns_kernel, rx_ns)
            self.sock_send.sendto(reply_data, (addr[0], port))

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
        _sr.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMPNS, 1)
        _sr.settimeout(self.timeout)
        _sr.bind((self.client_addr, self.client_port))
        self.sock_recv = _sr

        _ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        _ss.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMPING,
            SOF_TIMESTAMPING_TX_HARDWARE |
            SOF_TIMESTAMPING_TX_SOFTWARE |
            SOF_TIMESTAMPING_SOFTWARE
        )
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

            ts_now = time.time_ns()
            ts_now_kernel = ts_now
            for level, ctype, cdata in ancdata:
                if (
                    level == socket.SOL_SOCKET and
                    ctype == SO_TIMESTAMPNS
                ):
                    sec, nsec = struct.unpack("qq", cdata)
                    ts_now_kernel = sec * 1000000000 + nsec

            pad_bytes = len(data) - DATA_SIZE_REPLY
            seq, ts_tx, ts_rx_kernel, ts_rx = struct.unpack(f"!IQQQ{pad_bytes}x", data)
            ts_tx_kernel, ts_tx_kernel_hw, ts_tx_kernel_hw_raw = ring_pop(self.ring_buffer, seq)
            if ts_tx_kernel is None:
                ts_tx_kernel = ts_tx

            self.write_measurement({
                "sequence": seq,
                "client_schedule": ts_tx,
                "client_send": ts_tx_kernel,
                "client_send_hw": ts_tx_kernel_hw,
                "client_send_hw_raw": ts_tx_kernel_hw_raw,
                "server_recv": ts_rx_kernel,
                "server_process": ts_rx,
                "reply_recv": ts_now_kernel,
                "reply_process": ts_now,
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

            tx_ns = time.time_ns()
            send_data = struct.pack(f"!IIQ{self.padding}x", i, self.client_port, tx_ns)
            self.sock_send.sendto(send_data, (self.server_addr, self.server_port))
            
            data, ancdata, flags, addr = self.sock_send.recvmsg(
                1024, 1024, socket.MSG_ERRQUEUE
            )
            for level, ctype, data in ancdata:
                if level == socket.SOL_SOCKET and ctype == SO_TIMESTAMPING:
                    # data contains 3 timestamps:
                    # software, hardware transformed, raw hardware
                    ts = struct.unpack("6q", data)
                    sec1, nsec1, sec2, nsec2, sec3, nsec3 = ts # 2 and 3 are hardware tx_ts (if supported, otherwise zero)
                    tx_ns = sec1*1000000000 + nsec1
                    tx_ns2 = sec2*1000000000 + nsec2
                    if tx_ns2 == 0:
                        tx_ns2 = tx_ns
                    tx_ns3 = sec3*1000000000 + nsec3
                    if tx_ns3 == 0:
                        tx_ns3 = tx_ns2

                    ring_put(self.ring_buffer, i, tx_ns, tx_ns2, tx_ns3)
            time.sleep(interval)

        time.sleep(self.timeout)
        self.stop()

    def start(self, count=DEFAULT_COUNT, interval=DEFAULT_INTERVAL):
        if not self.outfile is None:
            self.file_object = open(self.outfile, "a")
        self.stop_event_send = Event()
        self.stop_event_recv = Event()
        self.ring_buffer = ring_create()

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
