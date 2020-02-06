import zmq
from helpers import get_my_addr
import threading
import argparse
import time

class Subscriber:
    def __init__(self, broker_ip, broker_port):
        self.broker_ip = broker_ip
        self.broker_port = broker_port
        self.pub_set = set()
        self.own_address = None
        self.sub_sock = None
        self.fp = None
        self.stopped = False
    
    def start(self):
        self.own_address = get_my_addr(self.broker_ip)
        self.fp = open("logs/sub_" + str(self.own_address[0]) + ":" + str(self.own_address[1]) + ".txt", "w")
        print('my address:\n' + str(self.own_address))

        def handler():
            self.sub_sock = zmq.Context().socket(zmq.SUB)
            sock = zmq.Context().socket(zmq.REP)
            sock.bind("tcp://*:" + str(self.own_address[1]))

            while True:
                if self.stopped == True:
                    break
                try:
                    content = sock.recv_string().split("#")
                    print("sub hander content:\n" + str(content))

                    if content[0] == "ADD":
                        if not content[2] in self.pub_set:
                            self.pub_set.add(content[2])
                            # ip, port = content[2].split(":")
                            print("connecting:\n" + "tcp://" + content[2])
                            self.sub_sock.connect("tcp://" + content[2])

                            sock.send_string("ADD_TOPIC")
                        else:
                            sock.send_string("ADD_NOTHING")
                    else:
                        sock.send_string("NOTHING")
                
                except Exception as ex:
                    print("something happened... " + str(ex))
                    break

        def playing():
            while True:
                if self.stopped == True:
                    break
                try:
                    recvContent = self.sub_sock.recv_string()
                    curTime = time.time()
                    self.fp.write(recvContent.split("#")[1] + "," + str(curTime) + "\n")
                    self.fp.flush()
                    print(self.sub_sock.recv_string())

                except Exception as ex:
                    print("exception occured when playing subs: " + ex)
            
        threading.Thread(target=handler).start()
        threading.Thread(target=playing).start()


    
    def register_sub(self, topic):
        print("***** register_sub *****")
        sock = zmq.Context().socket(zmq.REQ)
        sock.connect("tcp://" + str(self.broker_ip) + ":" + str(self.broker_port))
        sock.send_string("REG#" + str(self.own_address[0]) + ":" + str(self.own_address[1]) + "#" + topic)

        content = sock.recv_string().split("#")
        print("***** recv_string content:\n" + str(content) + "\n*****\n")
        if content[0] == "DONE_REG":
            for addr in eval(content[1]):
                if not addr in self.pub_set:
                    self.sub_sock.connect("tcp://" + addr)
                    self.pub_set.add(addr)
        
        self.sub_sock.setsockopt_string(zmq.SUBSCRIBE, topic)
    
    def stop():
        self.stopped = True


def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument ("-b", "--broker", type=str, default='10.0.0.1', help="broker's ip")
    parser.add_argument ("-p", "--port", type=int, default=5557, help="broker's port")
    parser.add_argument ("-n", "--number", type=int, default=10, help="number of topics")
    
    # parse the args
    args = parser.parse_args ()

    return args


if __name__ == "__main__":
    import random
    from time import sleep

    args = parseCmdLineArgs()
    my_sub = Subscriber(args.broker, args.port)
    my_sub.start()

    topic_set = set()

    for i in range(args.number):
        if random.randint(0, 2) > 0:
            my_sub.register_sub("topic" + str(i))
            topic_set.add(i)
    
    print("topic set is:\n" + str(topic_set))
