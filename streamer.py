# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct
from concurrent.futures import ThreadPoolExecutor
import time


class Streamer:

    def listener(self):
        while not self.closed:
            try:
                data, addr = self.socket.recvfrom()
                if data:
                    header = struct.unpack('i?', data[:self.header_size])
                    is_ack = header[1]
                    if is_ack:
                        ack_seq = header[0]
                        self.acks.append(ack_seq)
                    else:
                        self.recv_seq = header[0]
                        self.receive_buffer.append([self.recv_seq, data[self.header_size:]])
                        self.receive_buffer.sort(key=lambda x: x[0])
                        header = struct.pack('i?', self.recv_seq+1, True)  # int = 4 bytes, bool = 1 byte
                        packet = header + "ACK".encode()
                        self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            except Exception as e:
                print("listener died!")
                print(data[:5])
                print(e)

    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.receive_buffer = []
        self.sent_seq = 0
        self.recv_seq = -1
        self.next_seq = 0
        self.acks = []
        self.closed = False
        self.header_size = 5
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        message = []
        max_packet_size = 1472-self.header_size
        while len(data_bytes) >= max_packet_size:
            message.append(data_bytes[:max_packet_size])
            data_bytes = data_bytes[max_packet_size:]
        if len(data_bytes) > 0:
            message.append(data_bytes)
        for i in range(len(message)):
            header = struct.pack('i?', self.sent_seq + i, False)  # int = 4 bytes, bool = 1 byte
            packet = header + message[i]
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
        ack_seq = self.sent_seq
        self.sent_seq += len(message)
        while ack_seq+1 not in self.acks: time.sleep(0.01)
        self.acks.remove(ack_seq+1)

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        while len(self.receive_buffer) == 0 or self.receive_buffer[0][0] != self.next_seq:
            continue
        i = 0
        message = b""
        while i != len(self.receive_buffer) and (
                i == 0 or self.receive_buffer[i][0] == self.receive_buffer[i - 1][0] + 1):
            message += self.receive_buffer[i][1]
            i += 1
        self.receive_buffer = self.receive_buffer[i:]
        self.next_seq += i
        return message

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        self.closed = True
        self.socket.stoprecv()
