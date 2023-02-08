# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct

class Streamer:
    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.buffer = []
        self.seq = 0
        self.next_seq = 0

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        # Your code goes here!  The code below should be changed!
        message = []
        while len(data_bytes) >= 1472:
            message.append(data_bytes[:1472])
            data_bytes = data_bytes[1472:]
        if len(data_bytes) > 0:
            message.append(data_bytes)
        for i in range(len(message)):
            header = struct.pack('i', self.seq+i) # int = 4 bytes
            packet = header + message[i]
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
        self.seq += len(message)

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        # your code goes here!  The code below should be changed!
        seq = -1
        while seq != self.next_seq:
            data, addr = self.socket.recvfrom()
            seq = struct.unpack('i', data[:4])[0]
            self.buffer.append([seq, data[4:]])
        self.buffer.sort(key=lambda x: x[0])
        i = 0
        message = b""
        while i != len(self.buffer) and (i == 0 or self.buffer[i][0] == self.buffer[i-1][0]+1):
            message += self.buffer[i][1]
            i += 1
        self.buffer = self.buffer[i:]
        self.next_seq += i
        return message

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        # your code goes here, especially after you add ACKs and retransmissions.
        pass
