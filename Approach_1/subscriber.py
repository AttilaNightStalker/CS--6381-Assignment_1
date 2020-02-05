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

        threading.Thread(target=handler, args=self.own_address.split(":")).start()
    
    def register_sub(topic):
        print("***** register_sub *****)
        sock = Context().socket(zmq.REQ)
        sock.connect("tcp://" + str(self.broker_ip) + ":" + str(self.broker_port))
        sock.send("REG#" + self.own_address + "#" + topic)

        content = sock.recv().split("#")
        print("***** recv content:\n" + content + "*****\n")
        if content[0] == "DONE_REG":
            for addr in eval(content[1]):
                if not addr in self.pub_set:
                    self.sub_sock.connect("tcp://" + addr)
                    self.pub_set.add(addr)
        
        self.sub_sock.setsockopt(zmq.SUBSCRIBE, topic)



    
    def register_sub(topic)