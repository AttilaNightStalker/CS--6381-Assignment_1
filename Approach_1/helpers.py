import socket

def get_my_addr():
    tcp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    tcp.connect(('8.8.8.8', 80))
    return tcp.getsockname()