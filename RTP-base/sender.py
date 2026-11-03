"""
CS145 Reliable Transport Protocol (RTP-base)
Author: Nguyen Minh Chien (ZenCopper)
Implementation of Go-Back-N / Cumulative ACK reliable sender.
"""

import argparse
import select
import socket
import sys
import time
from utils import PacketHeader, compute_checksum

# Personal configuration constants
ZC_MAX_PAYLOAD = 1456  # 1472 - 16 bytes header
ZC_RTO_TIMEOUT = 0.5  # 500ms retransmission timer
ZC_HDR_LEN = 16

# Packet type identifiers
ZC_PKT_START = 0
ZC_PKT_END = 1
ZC_PKT_DATA = 2
ZC_PKT_ACK = 3


class ZenBaseSender:
    def __init__(self, target_host, target_port, win_capacity):
        self.target_endpoint = (target_host, target_port)
        self.win_capacity = win_capacity
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.setblocking(False)

        self.tx_buffer = {}
        self.total_chunks = 0
        self.send_base = 1
        self.next_seq = 1
        self.rto_timer = None

    def zc_craft_packet(self, p_type, seq_idx, payload=b""):
        p_len = len(payload) if p_type == ZC_PKT_DATA else 0
        hdr = PacketHeader(type=p_type, seq_num=seq_idx, length=p_len, checksum=0)
        hdr.checksum = compute_checksum(hdr / payload)
        return bytes(hdr / payload)

    def zc_unpack_packet(self, raw_data):
        if len(raw_data) < ZC_HDR_LEN:
            return None, None
        hdr = PacketHeader(raw_data[:ZC_HDR_LEN])
        if len(raw_data) < ZC_HDR_LEN + hdr.length:
            return None, None
        payload = raw_data[ZC_HDR_LEN : ZC_HDR_LEN + hdr.length]
        rx_checksum = hdr.checksum
        hdr.checksum = 0
        if rx_checksum != compute_checksum(hdr / payload):
            return None, None
        return hdr, payload

    def zc_handshake(self):
        """Perform 2-way START handshake."""
        start_packet = self.zc_craft_packet(ZC_PKT_START, 0)
        while True:
            self.udp_sock.sendto(start_packet, self.target_endpoint)
            rlist, _, _ = select.select([self.udp_sock], [], [], ZC_RTO_TIMEOUT)
            if rlist:
                try:
                    resp_bytes, _ = self.udp_sock.recvfrom(2048)
                except socket.error:
                    continue
                hdr, _ = self.zc_unpack_packet(resp_bytes)
                if hdr and hdr.type == ZC_PKT_ACK and hdr.seq_num == 1:
                    break

    def zc_prepare_data(self):
        stream_bytes = sys.stdin.buffer.read()
        if not stream_bytes:
            self.total_chunks = 0
            return

        offset = 0
        seq = 1
        while offset < len(stream_bytes):
            chunk = stream_bytes[offset : offset + ZC_MAX_PAYLOAD]
            self.tx_buffer[seq] = self.zc_craft_packet(ZC_PKT_DATA, seq, chunk)
            offset += ZC_MAX_PAYLOAD
            seq += 1
        self.total_chunks = len(self.tx_buffer)

    def zc_transmit_pipeline(self):
        while self.send_base <= self.total_chunks:
            # Transmit packets up to window boundary
            while (
                self.next_seq < self.send_base + self.win_capacity
                and self.next_seq <= self.total_chunks
            ):
                self.udp_sock.sendto(
                    self.tx_buffer[self.next_seq], self.target_endpoint
                )
                if self.rto_timer is None:
                    self.rto_timer = time.time()
                self.next_seq += 1

            # Monitor timeout
            if self.rto_timer is not None:
                duration = time.time() - self.rto_timer
                wait_time = ZC_RTO_TIMEOUT - duration
                if wait_time <= 0:
                    # Timeout triggered: Go-Back-N retransmission
                    for s in range(self.send_base, self.next_seq):
                        self.udp_sock.sendto(self.tx_buffer[s], self.target_endpoint)
                    self.rto_timer = time.time()
                    wait_time = ZC_RTO_TIMEOUT
                poll_delay = max(0.0, wait_time)
            else:
                poll_delay = 0.1

            rlist, _, _ = select.select([self.udp_sock], [], [], poll_delay)
            if rlist:
                while True:
                    try:
                        rx_data, _ = self.udp_sock.recvfrom(2048)
                    except socket.error:
                        break
                    hdr, _ = self.zc_unpack_packet(rx_data)
                    if hdr and hdr.type == ZC_PKT_ACK:
                        ack_val = hdr.seq_num
                        if ack_val > self.send_base:
                            self.send_base = ack_val
                            if self.send_base < self.next_seq:
                                self.rto_timer = time.time()
                            else:
                                self.rto_timer = None

    def zc_teardown(self):
        end_seq = self.total_chunks + 1
        end_packet = self.zc_craft_packet(ZC_PKT_END, end_seq)
        self.udp_sock.sendto(end_packet, self.target_endpoint)
        t_teardown = time.time()

        while True:
            left = ZC_RTO_TIMEOUT - (time.time() - t_teardown)
            if left <= 0:
                break
            rlist, _, _ = select.select([self.udp_sock], [], [], left)
            if rlist:
                try:
                    rx_data, _ = self.udp_sock.recvfrom(2048)
                except socket.error:
                    break
                hdr, _ = self.zc_unpack_packet(rx_data)
                if hdr and hdr.type == ZC_PKT_ACK:
                    break

        self.udp_sock.close()

    def run(self):
        self.zc_handshake()
        self.zc_prepare_data()
        self.zc_transmit_pipeline()
        self.zc_teardown()


def sender(receiver_ip, receiver_port, window_size):
    zen_sender = ZenBaseSender(receiver_ip, receiver_port, window_size)
    zen_sender.run()


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
