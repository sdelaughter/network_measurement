# Measure one-way delay components between devices
# Author: Samuel DeLaughter
# Last Modified: 2026-06-16

# OUTPUT
# Each line of output contains the following values
# sequence:             Sequence number
# client_schedule:      Time request is scheduled by the client
# client_send:          Time request is sent by the client kernel (software)
# client_send_hw:       Time request is sent by the client kernel (hardware_transformed), or =client_send if unsupported
# client_send_hw_raw:   Time request is sent by the client kernel (hardware_raw), or =client_send_hw if unsupported
# server_recv:          Time request is received by the server's kernel
# server_process:       Time request is processed by the server's program
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
#   python3 owd.py -A 10.0.0.1 -P 10000 -a 10.0.0.2 -p 10001 -i 0.1 -c 5 -t 2 -x 64 -o owd.csv

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
USE_NEW_TIMESTAMPS = True # TODO: Determine based on system architecture

# Packet size constants, to determine padding amounts
STRUCT_FORMAT_REQUEST = "!IQQ"
STRUCT_FORMAT_REPLY = "!IQQQQQ"
DATA_SIZE_REQUEST = len(struct.pack(STRUCT_FORMAT_REQUEST, 0, 0, 0))        # 16 bytes
DATA_SIZE_REPLY = len(struct.pack(STRUCT_FORMAT_REPLY, 0, 0, 0, 0, 0, 0))   # 44 bytes
AMPLIFICATION_PREVENTION = True # Pad request packets to the minimum size of reply packets (padding option is in addition to this)

# Socket constants
BUFF_SIZE = 1024        # TODO: compute actual space needed
ANC_BUFF_SIZE = 1024    # TODO: calculate with CMSG_SPACE()

# From include/uapi/asm-generic/socket.h
SO_TIMESTAMPNS_OLD = 35
SO_TIMESTAMPNS_NEW = 64
SO_TIMESTAMPNS = SO_TIMESTAMPNS_NEW if USE_NEW_TIMESTAMPS else SO_TIMESTAMPNS_OLD
SO_TIMESTAMPING_OLD = 37
SO_TIMESTAMPING_NEW = 65
SO_TIMESTAMPING = SO_TIMESTAMPING_NEW if USE_NEW_TIMESTAMPS else SO_TIMESTAMPING_OLD

# From include/uapi/linux/net_tstamp.h
SOF_TIMESTAMPING_TX_HARDWARE    = (1<<0)
SOF_TIMESTAMPING_TX_SOFTWARE    = (1<<1)
SOF_TIMESTAMPING_RX_HARDWARE    = (1<<2)
SOF_TIMESTAMPING_RX_SOFTWARE    = (1<<3)
SOF_TIMESTAMPING_SOFTWARE       = (1<<4)
SOF_TIMESTAMPING_SYS_HARDWARE   = (1<<5) # Deprecated
SOF_TIMESTAMPING_RAW_HARDWARE   = (1<<6)
SOF_TIMESTAMPING_OPT_ID         = (1<<7)
SOF_TIMESTAMPING_TX_SCHED       = (1<<8)
SOF_TIMESTAMPING_TX_ACK         = (1<<9)
SOF_TIMESTAMPING_OPT_CMSG       = (1<<10)
SOF_TIMESTAMPING_OPT_TSONLY     = (1<<11)
SOF_TIMESTAMPING_LAST = SOF_TIMESTAMPING_OPT_TSONLY
SOF_TIMESTAMPING_MASK = (SOF_TIMESTAMPING_LAST - 1) | SOF_TIMESTAMPING_LAST

# Ring buffer constants
RING_SIZE = 1024 # Max outstanding packets
RING_SLOT_SEQ = 0
RING_SLOT_TS0 = 1 # ts[0] is the software timestamp
RING_SLOT_TS1 = 2 # ts[1] is deprecated, implemented as a placeholder
RING_SLOT_TS2 = 3 # ts[2] is the hardware timestamp
RING_SLOTS = 4 # Number of values stored per packet

OUTPUT_ORDER = [
    "sequence",
    "client_schedule",
    "client_send",
    "client_send_hw",
    "server_recv_hw",
    "server_recv",
    "server_process",
    "server_reply",
    "reply_recv_hw",
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

def ring_put(ring, seq, ts0, ts1, ts2, size=RING_SIZE):
    i = (seq % size) * RING_SLOTS
    ring[i+RING_SLOT_SEQ] = seq
    ring[i+RING_SLOT_TS0] = ts0
    ring[i+RING_SLOT_TS1] = ts1
    ring[i+RING_SLOT_TS2] = ts2

def ring_pop(ring, seq, size=RING_SIZE):
    i = (seq % size) * RING_SLOTS
    if ring[i+RING_SLOT_SEQ] == seq:
        ts0 = ring[i+RING_SLOT_TS0]
        ts1 = ring[i+RING_SLOT_TS1]
        ts2 = ring[i+RING_SLOT_TS2]
        ring[i+RING_SLOT_SEQ] = -1
        return [ts0, ts1, ts2]
    return [None, None, None]

def get_kernel_time(cdata):
    sec, nsec = struct.unpack("qq", cdata)
    return sec * 1000000000 + nsec

def get_kernel_timing(cdata):
    # data contains 3 timestamps:
    # software, hardware transformed (deprecated), raw hardware
    ts = struct.unpack("6q", cdata)
    sec1, nsec1, sec2, nsec2, sec3, nsec3 = ts
    ts0 = sec1*1000000000 + nsec1
    # tx_ns1 = sec2*1000000000 + nsec2
    # if tx_ns1 == 0:
    #     tx_ns1 = tx_ns0
    ts1 = -1 # ts[1] is deprecated, ignore it
    ts2 = sec3*1000000000 + nsec3
    if ts2 == 0:
        ts2 = ts0
    return [ts0, ts1, ts2]

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
        # _sr.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMPNS, 1)
        _sr.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMPING,
            SOF_TIMESTAMPING_RX_HARDWARE |
            SOF_TIMESTAMPING_RX_SOFTWARE |
            SOF_TIMESTAMPING_SOFTWARE |
            SOF_TIMESTAMPING_RAW_HARDWARE
        )
        _sr.bind((server_addr, server_port))
        self.sock_recv = _sr

        # Catch interrupt/terminate signals to exit cleanly
        signal.signal(signal.SIGINT, self.exit)
        signal.signal(signal.SIGTERM, self.exit)

    def reply(self):
        while self.running:
            data, ancdata, msg_flags, addr = self.sock_recv.recvmsg(BUFF_SIZE, ANC_BUFF_SIZE)
            rx_ns = time.time_ns()
            rx_ns0 = rx_ns
            rx_ns2 = rx_ns
            for level, ctype, cdata in ancdata:
                if level == socket.SOL_SOCKET and ctype == SO_TIMESTAMPING:
                    rx_ns0, _, rx_ns2 = get_kernel_timing(cdata)

            pad_bytes = len(data) - DATA_SIZE_REQUEST
            i, port, tx_ns = struct.unpack(f"{STRUCT_FORMAT_REQUEST}{pad_bytes}x", data)
            reply_ns = time.time_ns()
            reply_data = struct.pack(f"{STRUCT_FORMAT_REPLY}{self.padding}x", i, tx_ns, rx_ns0, rx_ns2, rx_ns, reply_ns)
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
        # _sr.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMPNS, 1)
        _sr.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMPING,
            SOF_TIMESTAMPING_RX_HARDWARE |
            SOF_TIMESTAMPING_RX_SOFTWARE |
            SOF_TIMESTAMPING_SOFTWARE |
            SOF_TIMESTAMPING_RAW_HARDWARE
        )
        _sr.settimeout(self.timeout)
        _sr.bind((self.client_addr, self.client_port))
        self.sock_recv = _sr

        _ss = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        # _ss.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMPNS, 1)
        _ss.setsockopt(socket.SOL_SOCKET, SO_TIMESTAMPING,
            SOF_TIMESTAMPING_TX_HARDWARE |
            SOF_TIMESTAMPING_TX_SOFTWARE |
            SOF_TIMESTAMPING_SOFTWARE |
            SOF_TIMESTAMPING_RAW_HARDWARE
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
            ts_now_kernel0 = ts_now
            ts_now_kernel2 = ts_now
            for level, ctype, cdata in ancdata:
                if level == socket.SOL_SOCKET and ctype == SO_TIMESTAMPING:
                    ts_now_kernel0, _, ts_now_kernel2 = get_kernel_timing(cdata)

            pad_bytes = len(data) - DATA_SIZE_REPLY
            seq, ts_tx, ts_rx_kernel, ts_rx_kernel_hw, ts_rx, ts_reply = struct.unpack(f"{STRUCT_FORMAT_REPLY}{pad_bytes}x", data)
            ts_tx_kernel, _, ts_tx_kernel_hw = ring_pop(self.ring_buffer, seq) # ts[1] is deprecated, ignore it
            if ts_tx_kernel is None:
                ts_tx_kernel = ts_tx
            if ts_tx_kernel_hw is None:
                ts_tx_kernel_hw = ts_tx_kernel

            self.write_measurement({
                "sequence": seq,
                "client_schedule": ts_tx,
                "client_send": ts_tx_kernel,
                "client_send_hw": ts_tx_kernel_hw,
                "server_recv_hw": ts_rx_kernel_hw,
                "server_recv": ts_rx_kernel,
                "server_process": ts_rx,
                "server_reply": ts_reply,
                "reply_recv_hw": ts_now_kernel2,
                "reply_recv": ts_now_kernel0,
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
            pad_bytes = self.padding
            if AMPLIFICATION_PREVENTION:
                pad_bytes += max(0, DATA_SIZE_REPLY - DATA_SIZE_REQUEST)
            send_data = struct.pack(f"{STRUCT_FORMAT_REQUEST}{pad_bytes}x", i, self.client_port, tx_ns)
            self.sock_send.sendto(send_data, (self.server_addr, self.server_port))
            
            data, ancdata, flags, addr = self.sock_send.recvmsg(
                BUFF_SIZE, ANC_BUFF_SIZE, socket.MSG_ERRQUEUE
            )
            for level, ctype, cdata in ancdata:
                if level == socket.SOL_SOCKET and ctype == SO_TIMESTAMPING:
                    tx_ns0, tx_ns1, tx_ns2 = get_kernel_timing(cdata)
                    ring_put(self.ring_buffer, i, tx_ns0, tx_ns1, tx_ns2)
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
