import zmq
from helpers import get_my_addr
import threading

class Subscriber:
    def __init__(self, broker_ip, broker_port):
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        self.pub_set = set()
        self.own_address = None
        self.sub_sock = None
    
    def start(self):
        self.own_address = get_my_addr()

        def handler(my_ip, my_port):
            self.sub_sock = zmq.Context().socket(zmq.SUB)
            sock = zmq.Context().socket(zmq.REP)
            sock.bind("tcp://*:" + str(my_port))

            while True:
                try:
                    content = sock.recv().split("#")
                    print("sub hander content:\n" + str(content))

                    if content[0] == "ADD":
                        if not content[2] in self.pub_set:
                            self.pub_set.add(content[2])
                            # ip, port = content[2].split(":")
                            print("connecting:\n" + "tcp://" + content[2])
                            self.sub_sock.connect("tcp://" + content[2])

                            sock.send("ADD_TOPIC")
                        else:
                            sock.send("ADD_NOTHING")
                    else:
                        sock.send("NOTHING")
                
                except Exception as ex:
                    print("something happened... " + str(ex))
                    break

        def playing():
            while True:
                try:
                    print(self.sub_sock.recv())
                except Exception as ex:
                    print("exception occured when playing subs: " + ex)
            
        threading.Thread(target=handler, args=list(self.own_address)).start()
        threading.Thread(target=playing).start()


    
    def register_sub(self, topic):
        print("***** register_sub *****")
        sock = zmq.Context().socket(zmq.REQ)
        sock.connect("tcp://" + str(self.broker_ip) + ":" + str(self.broker_port))
        sock.send("REG#" + str(self.own_address[0]) + ":" + str(self.own_address[1]) + "#" + topic)

        content = sock.recv().split("#")
        print("***** recv content:\n" + str(content) + "\n*****\n")
        if content[0] == "DONE_REG":
            for addr in eval(content[1]):
                if not addr in self.pub_set:
                    self.sub_sock.connect("tcp://" + addr)
                    self.pub_set.add(addr)
        
        self.sub_sock.setsockopt(zmq.SUBSCRIBE, topic)
