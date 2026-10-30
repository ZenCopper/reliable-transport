import argparse
import select
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

CHUNK_SIZE = 1456
TIMEOUT = 0.5  # 500 ms


def make_packet(pkt_type, seq_num, payload=b""):
    length = len(payload) if pkt_type == 2 else 0
    header = PacketHeader(type=pkt_type, seq_num=seq_num, length=length, checksum=0)
    header.checksum = compute_checksum(header / payload)
    return bytes(header / payload)


def parse_packet(raw_bytes):
    if len(raw_bytes) < 16:
        return None, None
    header = PacketHeader(raw_bytes[:16])
    if len(raw_bytes) < 16 + header.length:
        return None, None
    payload = raw_bytes[16 : 16 + header.length]
    pkt_checksum = header.checksum
    header.checksum = 0
    if pkt_checksum != compute_checksum(header / payload):
        return None, None
    return header, payload


def sender(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setblocking(False)

    dest_addr = (receiver_ip, receiver_port)

    # 1. Connection initiation: Send START (type=0, seq_num=0)
    start_pkt = make_packet(0, 0, b"")
    while True:
        s.sendto(start_pkt, dest_addr)
        readable, _, _ = select.select([s], [], [], TIMEOUT)
        if readable:
            try:
                data, _ = s.recvfrom(2048)
            except socket.error:
                continue
            header, _ = parse_packet(data)
            if header and header.type == 3 and header.seq_num == 1:
                break

    # 2. Read data from stdin and split into chunks
    data = sys.stdin.buffer.read()
    chunks = (
        [data[i : i + CHUNK_SIZE] for i in range(0, len(data), CHUNK_SIZE)]
        if data
        else []
    )
    total_packets = len(chunks)

    # Pre-build DATA packets (seq_num starts from 1)
    packets = {}
    for i, chunk in enumerate(chunks):
        seq = i + 1
        packets[seq] = make_packet(2, seq, chunk)

    # 3. Sliding window transmission (Cumulative ACK / Go-Back-N)
    base = 1
    next_seq_num = 1
    timer_start = None

    while base <= total_packets:
        # Send packets within the sliding window
        while next_seq_num < base + window_size and next_seq_num <= total_packets:
            s.sendto(packets[next_seq_num], dest_addr)
            if timer_start is None:
                timer_start = time.time()
            next_seq_num += 1

        # Check retransmission timer
        if timer_start is not None:
            elapsed = time.time() - timer_start
            remaining = TIMEOUT - elapsed
            if remaining <= 0:
                # Timer expired: retransmit all packets in the current window
                for seq in range(base, next_seq_num):
                    s.sendto(packets[seq], dest_addr)
                timer_start = time.time()
                remaining = TIMEOUT
            select_timeout = max(0.0, remaining)
        else:
            select_timeout = 0.1

        # Wait for incoming ACKs or timeout
        readable, _, _ = select.select([s], [], [], select_timeout)
        if readable:
            while True:
                try:
                    recv_data, _ = s.recvfrom(2048)
                except socket.error:
                    break
                header, _ = parse_packet(recv_data)
                if header and header.type == 3:  # ACK packet
                    ack_num = header.seq_num
                    if ack_num > base:
                        base = ack_num
                        if base < next_seq_num:
                            timer_start = time.time()
                        else:
                            timer_start = None

    # 4. Connection teardown: Send END (type=1, seq_num=total_packets+1)
    end_seq_num = total_packets + 1
    end_pkt = make_packet(1, end_seq_num, b"")
    s.sendto(end_pkt, dest_addr)
    end_start_time = time.time()

    # Wait up to 500ms for ACK of END
    while True:
        elapsed = time.time() - end_start_time
        remaining = TIMEOUT - elapsed
        if remaining <= 0:
            break
        readable, _, _ = select.select([s], [], [], remaining)
        if readable:
            try:
                recv_data, _ = s.recvfrom(2048)
            except socket.error:
                break
            header, _ = parse_packet(recv_data)
            if header and header.type == 3:
                # Received ACK of END
                break

    s.close()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "receiver_ip", help="The IP address of the host that receiver is running on"
    )
    parser.add_argument(
        "receiver_port", type=int, help="The port number on which receiver is listening"
    )
    parser.add_argument(
        "window_size", type=int, help="Maximum number of outstanding packets"
    )
    args = parser.parse_args()

    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
