import zmq
from helpers import get_my_addr
import threading

class Subscriber:
    def __init__(self, broker_ip, broker_port):
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        self.pub_set = {}
        self.own_address = None
        self.sub_sock = None
    
    def start(self):
        self.own_address = get_my_addr()
        self.sub_sock = zmq.Context().socket(zmq.SUB)

        def handler(my_ip, my_port):
            sock = zmq.Context().socket(zmq.REP)
            sock.bind("tcp://*" + str(my_port))

            while True:
                try:
                    content = sock.recv().split("#")
                    if content[0] == "ADD":
                        if not content[2] in self.pub_set:
                            pub_set.add(content[2])
                            ip, port = content[2].split(":")
                            self.sub_sock.connect("tcp://" + ip + ":" + port)
                
                except Exception as ex:
                    print("something happened... " + str(ex))
        
        threading.Thread




    
    def register_sub(topic)