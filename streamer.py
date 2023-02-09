# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct
from concurrent.futures import ThreadPoolExecutor


class Streamer:

    def listener(self):
        while not self.closed:
            try:
                data, addr = self.socket.recvfrom()
                if data != b"":
                    self.recv_seq = struct.unpack('i', data[:4])[0]
                    self.receive_buffer.append([self.recv_seq, data[4:]])
                    self.receive_buffer.sort(key=lambda x: x[0])
            except Exception as e:
                print("listener died!")
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
        self.closed = False
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        message = []
        while len(data_bytes) >= 1468:
            message.append(data_bytes[:1468])
            data_bytes = data_bytes[1468:]
        if len(data_bytes) > 0:
            message.append(data_bytes)
        for i in range(len(message)):
            header = struct.pack('i', self.sent_seq + i)  # int = 4 bytes
            packet = header + message[i]
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
        self.sent_seq += len(message)

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
