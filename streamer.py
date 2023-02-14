# do not import anything else from loss_socket besides LossyUDP
from lossy_socket import LossyUDP
# do not import anything else from socket except INADDR_ANY
from socket import INADDR_ANY
import struct
from concurrent.futures import ThreadPoolExecutor
import time
import hashlib
import threading


class Streamer:

    def __init__(self, dst_ip, dst_port,
                 src_ip=INADDR_ANY, src_port=0):
        """Default values listen on all network interfaces, chooses a random source port,
           and does not introduce any simulated packet loss."""
        self.socket = LossyUDP()
        self.socket.bind((src_ip, src_port))
        self.dst_ip = dst_ip
        self.dst_port = dst_port
        self.recv_buff = []  # received packets after first gap [seq, data]
        self.sent_seq = 0  # all packets up to this number have been sent
        self.next_seq = 0  # lowest seq not yet received
        self.closed = False
        self.header_size = 6
        self.fin_ack = False
        self.fin_recv = False
        self.fin_sent = False
        self.ack_seq = 0
        self.lock = threading.Lock()
        self.ack_time = 0
        self.no_ack = [] # remove once window implemented
        self.send_buff = []
        self.window_size = 10
        self.window = []
        self.num_acks = 0
        executor = ThreadPoolExecutor(max_workers=3)
        executor.submit(self.listener)
        executor.submit(self.sender)
        executor.submit(self.resend)

    def hash_send(self, packet):
        raw = hashlib.sha1(packet).digest() + packet
        self.socket.sendto(raw, (self.dst_ip, self.dst_port))

    def listener(self):
        while not self.closed:
            try:
                raw, addr = self.socket.recvfrom()
                check = raw[:20]
                data = raw[20:]
                if check == hashlib.sha1(data).digest() and data:
                    header = struct.unpack('i??', data[:self.header_size])
                    is_ack = header[1]
                    is_fin = header[2]
                    if is_ack:
                        if is_fin:  # if FIN ACK, set fin_ack
                            self.fin_ack = True
                        else:  # if ACK
                            if header[0] != self.ack_seq:
                                self.ack_time = time.time()
                                self.ack_seq = header[0]
                                self.num_acks = 0
                            else:
                                self.num_acks += 1
                            self.no_ack = [packet for packet in self.no_ack if packet[0] >= self.ack_seq]
                    elif is_fin:  # if FIN, send FIN ACK and set fin_recv
                        self.fin_recv = True
                        header = struct.pack('i??', 0, True, True)
                        packet = header + "ACK".encode()
                        self.hash_send(packet)
                    else:
                        recv_seq = header[0]
                        recv_log = [recv_seq, data[self.header_size:]]
                        header = struct.pack('i??', self.next_seq, True, False)  # int = 4 bytes, bool = 1 byte
                        packet = header + "ACK".encode()
                        self.hash_send(packet)
                        if recv_seq == self.next_seq:
                            with self.lock:
                                self.next_seq += 1
                                self.recv_buff.append(recv_log)
                        print(f"sent ACK for {self.next_seq}")
            except Exception as e:
                print("listener died!")
                print(e)

    def resend(self):
        while len(self.window) != 0:
            try:
                """Check if ACK received within timeout interval"""
                if time.time() - self.ack_time > 0.1:
                    for pair in self.window:
                        self.hash_send(pair[1])
                        print(f"retrying for {pair[1][0]}")
            except Exception as e:
                print("resend died!")
                print(e)

    def sender(self):
        while not self.closed:
            if not len(self.send_buff) == len(self.window) == 0:
                try:
                    while len(self.window) < self.window_size and len(self.send_buff) != 0:
                        new_pair = self.send_buff[0]
                        self.send_buff.pop(0)
                        self.window.append(new_pair)
                        self.hash_send(new_pair[1])
                    """While first element has been ACKed or window isn't full, pop and send new pair"""
                    while len(self.window) != 0 and self.ack_seq > self.window[0][0]:
                        self.window.pop(0)
                        if len(self.send_buff) != 0:
                            new_pair = self.send_buff[0]
                            self.send_buff.pop(0)
                            self.window.append(new_pair)
                            self.hash_send(new_pair[1])
                except Exception as e:
                    print("sender died!")
                    print(e)

    def send(self, data_bytes: bytes) -> None:
        """Note that data_bytes can be larger than one packet."""
        message = []
        """Chunk message into packets"""
        max_packet_size = 1452-self.header_size
        while len(data_bytes) >= max_packet_size:
            message.append(data_bytes[:max_packet_size])
            data_bytes = data_bytes[max_packet_size:]
        if len(data_bytes) > 0:
            message.append(data_bytes)
        """Put packets in queue"""
        for i in range(len(message)):
            header = struct.pack('i??', self.sent_seq + i, False, False)  # int = 4 bytes, bool = 1 byte
            packet = header + message[i]
            self.send_buff.append([self.sent_seq + i, packet])
            self.sent_seq += 1

    def recv(self) -> bytes:
        """Blocks (waits) if no data is ready to be read from the connection."""
        """While buffer empty or gap at beginning of buffer"""
        while len(self.recv_buff) == 0:
            continue
        with self.lock:
            message = self.recv_buff[0][1]
            self.recv_buff.pop(0)
            return message

    def close(self) -> None:
        """Cleans up. It should block (wait) until the Streamer is done with all
           the necessary ACKs and retransmissions"""
        while len(self.no_ack) != 0:
            for pair in self.no_ack:
                if self.ack_seq == pair[0]:
                    self.hash_send(pair[1])
                    print(f"retrying for {pair[1][0]}")
        """Send FIN when all sent data ACKed"""
        header = struct.pack('i??', self.next_seq, False, True)  # int =  4 bytes, bool = 1 byte
        packet = header + "FIN".encode()
        self.hash_send(packet)
        time_sent = time.time()
        """Wait for FIN ACK and resend FIN if timer runs out"""
        while not self.fin_ack:
            if time.time() - time_sent > 0.25:
                self.hash_send(packet)
            time.sleep(0.1)
        """Wait until listener gets FIN"""
        while not self.fin_recv:
            continue
        time.sleep(2)
        self.closed = True
        self.socket.stoprecv()
