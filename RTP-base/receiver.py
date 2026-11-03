"""
CS145 Reliable Transport Protocol (RTP-base)
Author: Nguyen Minh Chien (ZenCopper)
Implementation of Cumulative ACK reliable receiver.
"""

import argparse
import socket
import sys
from utils import PacketHeader, compute_checksum

ZC_HDR_LEN = 16
ZC_PKT_START = 0
ZC_PKT_END = 1
ZC_PKT_DATA = 2
ZC_PKT_ACK = 3


class ZenBaseReceiver:
    def __init__(self, bind_ip, bind_port, win_capacity):
        self.bind_ip = bind_ip
        self.bind_port = bind_port
        self.win_capacity = win_capacity
        self.udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.udp_sock.bind((self.bind_ip, self.bind_port))

        self.conn_active = False
        self.remote_peer = None
        self.rcv_next = 1
        self.out_of_order_buf = {}
        self.end_flag = False
        self.end_seq_val = None

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

    def zc_listen_loop(self):
        while True:
            raw_pkt, peer_addr = self.udp_sock.recvfrom(2048)
            hdr, payload = self.zc_unpack_packet(raw_pkt)
            if hdr is None:
                continue

            if not self.conn_active:
                if hdr.type == ZC_PKT_START:
                    self.remote_peer = peer_addr
                    self.conn_active = True
                    self.rcv_next = 1
                    self.out_of_order_buf.clear()
                    self.end_flag = False
                    self.end_seq_val = None
                    ack_start = self.zc_craft_packet(ZC_PKT_ACK, 1)
                    self.udp_sock.sendto(ack_start, self.remote_peer)
                continue

            if peer_addr != self.remote_peer or hdr.type == ZC_PKT_START:
                continue

            if hdr.type == ZC_PKT_DATA:
                seq = hdr.seq_num
                if seq >= self.rcv_next + self.win_capacity:
                    continue

                if seq < self.rcv_next:
                    ack_dup = self.zc_craft_packet(ZC_PKT_ACK, self.rcv_next)
                    self.udp_sock.sendto(ack_dup, self.remote_peer)
                elif seq > self.rcv_next:
                    self.out_of_order_buf[seq] = payload
                    ack_unord = self.zc_craft_packet(ZC_PKT_ACK, self.rcv_next)
                    self.udp_sock.sendto(ack_unord, self.remote_peer)
                else:
                    sys.stdout.buffer.write(payload)
                    self.rcv_next += 1
                    while self.rcv_next in self.out_of_order_buf:
                        sys.stdout.buffer.write(
                            self.out_of_order_buf.pop(self.rcv_next)
                        )
                        self.rcv_next += 1
                    sys.stdout.buffer.flush()

                    cum_ack = self.zc_craft_packet(ZC_PKT_ACK, self.rcv_next)
                    self.udp_sock.sendto(cum_ack, self.remote_peer)

                    if self.end_flag and self.rcv_next == self.end_seq_val:
                        end_ack = self.zc_craft_packet(ZC_PKT_ACK, self.end_seq_val)
                        self.udp_sock.sendto(end_ack, self.remote_peer)
                        break

            elif hdr.type == ZC_PKT_END:
                if hdr.seq_num == self.rcv_next:
                    end_ack = self.zc_craft_packet(ZC_PKT_ACK, hdr.seq_num)
                    self.udp_sock.sendto(end_ack, self.remote_peer)
                    sys.stdout.buffer.flush()
                    break
                elif hdr.seq_num > self.rcv_next:
                    self.end_flag = True
                    self.end_seq_val = hdr.seq_num
                    ack_pkt = self.zc_craft_packet(ZC_PKT_ACK, self.rcv_next)
                    self.udp_sock.sendto(ack_pkt, self.remote_peer)

        self.udp_sock.close()


def receiver(receiver_ip, receiver_port, window_size):
    zen_receiver = ZenBaseReceiver(receiver_ip, receiver_port, window_size)
    zen_receiver.zc_listen_loop()


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

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
