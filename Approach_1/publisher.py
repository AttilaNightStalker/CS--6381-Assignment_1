from helpers import get_my_addr
import zmq
import argparse
import time

class Publisher:
    def __init__(self, broker_ip, broker_port):
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        self.topic_set = set()
        self.own_address = None
        self.pub_sock = None
        self.fp = None
        # self.stopped = False
    
    def register_pub(self, topic):
        if self.own_address is None:
            self.own_address = get_my_addr(self.broker_ip)
        if self.fp == None:
            self.fp = open("logs/pub_" + str(self.own_address[0]) + ":" + str(self.own_address[1]) + ".txt", "w")
        
        if not topic in self.topic_set:
            sock = zmq.Context().socket(zmq.REQ)
            sock.connect("tcp://" + str(self.broker_ip) + ":" + str(self.broker_port))
            sock.send_string("REG#" + str(self.own_address[0]) + ":" + str(self.own_address[1]) + "#" + topic)
            assert(sock.recv_string() == "DONE_REG")

            sock.close()
            self.topic_set.add(topic)
    
    def publish(self, topic, content):
        if not topic in self.topic_set:
            print("topic should be registered before publishing")
            return
        
        if self.pub_sock == None:
            self.pub_sock = zmq.Context().socket(zmq.PUB)
            self.pub_sock.bind("tcp://*:" + str(self.own_address[1]))
        
        self.pub_sock.send_string(topic + "#" + content)
        curTime = time.time()
        self.fp.write(content + "," + str(curTime) + "\n")
        self.fp.flush()
        
        

def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument ("-b", "--broker", type=str, default='10.0.0.1', help="broker's ip")
    parser.add_argument ("-p", "--port", type=int, default=5556, help="broker's port")
    parser.add_argument ("-n", "--number", type=int, default=10, help="number of topics")
    parser.add_argument ("-t", "--times", type=int, default=60, help="publishing times")
    
    # parse the args
    args = parser.parse_args ()

    return args


# def pubRun(broker, port, number, times)
#     import random
#     from time import sleep

#     # args = parseCmdLineArgs()
#     my_pub = Publisher(broker, port)

#     topic_set = set()

#     for i in range(number):
#         if random.randint(0, 2) == 0:
#             my_pub.register_pub("topic" + str(i))
#             topic_set.add(i)
    
#     print("topic set is:\n" + str(topic_set))
    
#     cnt = args.time
#     while cnt > 0:
#         topic_n = random.randint(1, number) - 1
#         if topic_n in topic_set:
#             my_pub.publish("topic" + str(topic_n), str(random.getrandbits(128)))
#             cnt -= 1
#             print("publish left: " + str(cnt))
#             sleep(0.5)

if __name__ == "__main__":
    import random
    from time import sleep

    args = parseCmdLineArgs()
    my_pub = Publisher(args.broker, args.port)

    topic_set = set()

    for i in range(args.number):
        if random.randint(0, 2) == 0:
            my_pub.register_pub("topic" + str(i))
            topic_set.add(i)
    
    print("topic set is:\n" + str(topic_set))
    
    cnt = args.times
    while cnt > 0:
        topic_n = random.randint(1, args.number) - 1
        if topic_n in topic_set:
            my_pub.publish("topic" + str(topic_n), str(random.getrandbits(128)))
            cnt -= 1
            print("publish left: " + str(cnt))
            sleep(0.5)

        

    