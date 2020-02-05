from helpers import get_my_addr

class Publisher:
    def __init__(self, broker_ip, broker_port):
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        self.topic_set = {}
        self.own_address = None
        self.pub_sock = None
    
    def register_pub(self, topic):
        if self.own_address == None:
            self.own_address = get_my_addr()
        
        if not topic in self.topic_set:
            sock = zmq.Context().socket(zmq.REQ)
            sock.connect("tcp://" + str(self.broker_ip) + ":" + str(self.broker_port))
            sock.send("REG#" + self.own_address + "#" + topic)
            assert(sock.recv() == "DONE_REG")

            sock.close()
            self.topic_set.add(topic)
    
    def publish(self, topic, content):
        if not topic in self.topic_set:
            print("topic should be registered before publishing")
            return
        
        if self.pub_sock == None:
            self.pub_sock = zmq.Context().socket(zmq.PUB)
            self.pub_sock.bind("tcp://*:" + self.own_address.split(":")[1])
        
        self.pub_sock.send(topic + "#" + content)

