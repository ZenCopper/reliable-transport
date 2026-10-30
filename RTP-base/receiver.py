import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum


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


def receiver(receiver_ip, receiver_port, window_size):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((receiver_ip, receiver_port))

    state = "LISTENING"
    sender_addr = None
    expected_seq_num = 1
    buffer = {}
    end_received = False
    end_seq_num = None

    while True:
        pkt, address = s.recvfrom(2048)
        header, msg = parse_packet(pkt)
        if header is None:
            # Checksum mismatch or corrupted packet: drop packet
            continue

        if state == "LISTENING":
            if header.type == 0:  # START packet
                sender_addr = address
                state = "CONNECTED"
                expected_seq_num = 1
                buffer.clear()
                end_received = False
                end_seq_num = None
                # Send ACK for START with seq_num = 1
                ack_pkt = make_packet(3, 1, b"")
                s.sendto(ack_pkt, sender_addr)
            continue

        # In CONNECTED state: ignore START packets or other senders
        if address != sender_addr:
            continue
        if header.type == 0:
            continue

        if header.type == 2:  # DATA packet
            seq = header.seq_num
            # Drop packet if seq_num >= expected_seq_num + window_size
            if seq >= expected_seq_num + window_size:
                continue

            if seq < expected_seq_num:
                # Duplicate packet: send cumulative ACK with current expected_seq_num
                ack_pkt = make_packet(3, expected_seq_num, b"")
                s.sendto(ack_pkt, sender_addr)
            elif seq > expected_seq_num:
                # Out-of-order packet within window: buffer and send cumulative ACK
                buffer[seq] = msg
                ack_pkt = make_packet(3, expected_seq_num, b"")
                s.sendto(ack_pkt, sender_addr)
            else:  # seq == expected_seq_num
                sys.stdout.buffer.write(msg)
                expected_seq_num += 1
                while expected_seq_num in buffer:
                    sys.stdout.buffer.write(buffer.pop(expected_seq_num))
                    expected_seq_num += 1
                sys.stdout.buffer.flush()

                ack_pkt = make_packet(3, expected_seq_num, b"")
                s.sendto(ack_pkt, sender_addr)

                # If END was received earlier and in-order stream reached end_seq_num
                if end_received and expected_seq_num == end_seq_num:
                    end_ack = make_packet(3, end_seq_num, b"")
                    s.sendto(end_ack, sender_addr)
                    break

        elif header.type == 1:  # END packet
            if header.seq_num == expected_seq_num:
                end_ack = make_packet(3, header.seq_num, b"")
                s.sendto(end_ack, sender_addr)
                sys.stdout.buffer.flush()
                break
            elif header.seq_num > expected_seq_num:
                end_received = True
                end_seq_num = header.seq_num
                ack_pkt = make_packet(3, expected_seq_num, b"")
                s.sendto(ack_pkt, sender_addr)

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

    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
