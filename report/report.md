# CS 145 Project 4: Reliable Transport Protocol (RTP)

## Author and collaborators
### Author name
- **Name:** Nguyen Minh Chien (ZenCopper)
- **Email:** nuyenminhchien2021tb@gmail.com

### Collaborators
- None. This project was completed independently.

## Report
### 1. RTP-base Implementation
- Built on top of standard non-blocking UDP sockets (`socket.SOCK_DGRAM`) and `select.select`.
- Structured with a modular class `ZenBaseSender` and `ZenBaseReceiver`.
- Uses Go-Back-N / Cumulative Acknowledgement protocol:
  - Connection is initialized using a two-way handshake (`START` type 0, `ACK` type 3 with `seq_num=1`).
  - Data segments are chunked into 1456-byte blocks, fitted into 1472-byte UDP datagrams with 16-byte `PacketHeader`.
  - A 500ms retransmission timer (`ZC_RTO_TIMEOUT = 0.5`) manages unacknowledged frames within the sliding window.
  - Receiver checks 32-bit CRC checksums, buffers out-of-order packets, and sends cumulative ACKs indicating the next expected sequence number.
  - Connection teardown is handled gracefully using `END` (type 1) and a 500ms wait for the final ACK.

### 2. RTP-opt Implementation
- Implemented in `ZenOptSender` and `ZenOptReceiver`.
- Replaces cumulative acknowledgements with Selective Acknowledgement (Selective Repeat):
  - Receiver sends ACKs carrying the exact `seq_num` of each received data packet.
  - Sender tracks acknowledged packet sequence numbers using `self.acked_seqs`.
  - Upon a 500ms timeout on the current window, only unacknowledged packets (`s not in self.acked_seqs`) are retransmitted, drastically reducing redundant network traffic under packet loss and reordering conditions.

## Citations
- Harvard CS145 Course Materials and Scapy Documentation.
- Python Socket and Select standard library documentation.

## Grading notes (if any)
- Fully passes all automated tests across varying window sizes (1, 5, 32, 128) under all four simulated network impairment modes (`0123`: Delay, Reorder, Drop, Jam/Corruption).
