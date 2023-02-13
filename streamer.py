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
                    header = struct.unpack('i??', data[:self.header_size])
                    is_ack = header[1]
                    is_fin = header[2]
                    if is_ack:
                        if is_fin:  # if FIN ACK, set fin_ack
                            self.fin_ack = True
                        else:  # if ACK
                            ack_seq = header[0]
                            if ack_seq in self.no_ack:
                                self.no_ack.remove(ack_seq)
                    elif is_fin: # if FIN, send FIN ACK and set fin_recv
                        self.fin_recv = True
                        header = struct.pack('i??', 0, True, True)
                        packet = header + "ACK".encode()
                        self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                    else:
                        recv_seq = header[0]
                        recv_log = [recv_seq, data[self.header_size:]]
                        if recv_log not in self.receive_buffer and recv_seq >= self.next_seq:
                            self.receive_buffer.append(recv_log)
                        self.receive_buffer.sort(key=lambda x: x[0])
                        header = struct.pack('i??', recv_seq+1, True, False)  # int = 4 bytes, bool = 1 byte
                        packet = header + "ACK".encode()
                        self.socket.sendto(packet, (self.dst_ip, self.dst_port))
                        # print(f"sent ack for {recv_seq}")
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
        self.receive_buffer = []  # received packets after first gap [seq, data]
        self.sent_seq = 0  # all packets up to this number have been sent
        self.next_seq = 0  # lowest seq not yet received
        self.closed = False
        self.header_size = 6
        self.fin_ack = False
        self.fin_recv = False
        self.fin_sent = False
        self.no_ack = []
        executor = ThreadPoolExecutor(max_workers=1)
        executor.submit(self.listener)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        message = []
        """Chunk message into packets"""
        max_packet_size = 1472-self.header_size
        while len(data_bytes) >= max_packet_size:
            message.append(data_bytes[:max_packet_size])
            data_bytes = data_bytes[max_packet_size:]
        if len(data_bytes) > 0:
            message.append(data_bytes)
        packets = []
        message_acks = []
        """Send packets and record time sent"""
        for i in range(len(message)):
            header = struct.pack('i??', self.sent_seq + i, False, False)  # int = 4 bytes, bool = 1 byte
            packet = header + message[i]
            packets.append(packet)
            self.no_ack.append(self.sent_seq + i + 1)
            self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            message_acks.append([self.sent_seq + i + 1, time.time()])
        """Wait until all messages are ACKed"""
        while set([i[0] for i in message_acks]) & set(self.no_ack):
            for i in [j for j in message_acks if j[0] in self.no_ack]:  # will have to change in part 5
                if time.time()-i[1] > 0.25:
                    self.socket.sendto(packets[i[0]-self.sent_seq-1], (self.dst_ip, self.dst_port))
                    i[1] = time.time()
            time.sleep(0.01)
        """Remove ACKs of completed messages from self.acks"""
        self.sent_seq += len(message)

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        """While buffer empty or gap at beginning of buffer"""
        while len(self.receive_buffer) == 0 or self.receive_buffer[0][0] != self.next_seq:
            continue
        i = 0
        message = b""
        """While i's seq # is 1 more than i-1's seq #"""
        while i != len(self.receive_buffer) and (
                i == 0 or self.receive_buffer[i][0] == self.receive_buffer[i - 1][0] + 1):
            message += self.receive_buffer[i][1]
            i += 1
        self.receive_buffer = self.receive_buffer[i+1:]
        self.next_seq += i
        return message

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        while len(self.no_ack) != 0:
            continue
        """Send FIN when all sent data ACKed"""
        header = struct.pack('i??', 0, False, True)  # int = 4 bytes, bool = 1 byte
        packet = header + "FIN".encode()
        self.socket.sendto(packet, (self.dst_ip, self.dst_port))
        time_sent = time.time()
        """Wait for FIN ACK and resend FIN if timer runs out"""
        while not self.fin_ack:
            if time.time() - time_sent > 0.25:
                self.socket.sendto(packet, (self.dst_ip, self.dst_port))
            time.sleep(0.1)
        """Wait until listener gets FIN"""
        while not self.fin_recv:
            continue
        time.sleep(2)
        self.closed = True
        self.socket.stoprecv()
