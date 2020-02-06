import socket

def get_my_addr(test_dst):
    tcp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    tcp.connect((test_dst, 80))
    return tcp.getsockname()