import zmq
import threading
import argparse

global_thr = []

def signal_handler(signal, frame):
    print('You pressed Ctrl+C!')
    for t in global_thr:
        t.terminate()
    sys.exit(0)

class Broker:
    def __init__(self, pub_port, sub_port):
        self.topic_pub_map = {}
        self.topic_sub_map = {}
        self.pub_topic_map = {}
        self.sub_topic_map = {}

        # self.ip = ip
        self.pub_port = pub_port
        self.sub_port = sub_port

    
    def register_pub(self, topic, address):
        print("****** registering pub *******")
        if address in self.pub_topic_map:
            self.pub_topic_map[address].add(topic)
        else:
            self.pub_topic_map[address] = {topic}
        
        if topic in self.topic_pub_map:
            self.topic_pub_map[topic].add(address)
        else:
            self.topic_pub_map[topic] = {address}
        
        if topic in self.topic_sub_map:
            msg_sock = zmq.Context().socket(zmq.REQ)
            for sub_addr in self.topic_sub_map[topic]:
                print("sub addr:\n" + sub_addr)

                msg_sock.connect("tcp://" + sub_addr)
                msg_sock.send_string("ADD#" + topic + "#" + address)
                print(msg_sock.recv_string())
            msg_sock.close()

    
    def validate_pub(self, topic, address):
        if address in self.pub_topic_map:
            if topic in self.pub_topic_map[address]:
                return "OK"
            return "NO_TOPIC"
        return "NO_PUBLISHER"
    

    def cancel_pub(self, topic, address):
        print("****** cancel pub ******")
        try:
            self.topic_pub_map[topic].remove(address)
        except KeyError:
            print("no address-" + address + " in topic-" + topic)
        
        try:
            self.pub_topic_map[address].remove(topic)
        except KeyError:
            print("no topic-" + topic + " in address-" + address)

        if topic in self.topic_sub_map:
            msg_sock = zmq.Context().socket(zmq.REQ)
            for sub_addr in self.topic_sub_map[topic]:
                msg_sock.connect(sub_addr)
                msg_sock.send_string("DEL#" + topic + "#" + address)
                print(msg_sock.recv_string())
            msg_sock.close()
    

    def register_sub(self, topic, address):
        print("***** registering sub *****")
        if address in self.sub_topic_map:
            self.sub_topic_map[address].add(topic)
        else:
            self.pub_topic_map[address] = {topic}
        
        if topic in self.topic_sub_map:
            self.topic_sub_map[topic].add(address)
        else:
            self.topic_sub_map[topic] = {address}
        
        if topic in self.topic_pub_map:
            return list(self.topic_pub_map[topic])
        else:
            return []
    

    def cancel_sub(self, topic, address):
        print("****** cancel sub *******")
        try:
            self.topic_sub_map[topic].remove(address)
        except KeyError:
            print("no address-" + address + " in topic-" + topic)
        
        try:
            self.sub_topic_map[address].remove(topic)
        except KeyError:
            print("no topic-" + topic + " in address-" + address)


    def get_pub_thread(self):
        print("pub handler")
        def thr_body(port):
            sock = zmq.Context().socket(zmq.REP)
            sock.bind("tcp://*:" + str(port))

            while True:
                print('waiting for pub ... ')
                try:
                    content = sock.recv_string()
                    print("raw msg from pub port:\n" + content)
                    content = content.split("#")
                    print("splited content:\n" + str(content))
                    if content[0] == "REG":
                        print("is reg")
                        self.register_pub(content[2], content[1])
                        print("sending")
                        sock.send_string("DONE_REG")
                    elif content[0] == "CCL":
                        self.cancel_pub(content[2], content[1])
                        sock.send_string("DONE_CCL")
                    elif content[0] == "VAL":
                        sock.send_string(self.validate_pub(content[2], content[1]))
                except IndexError:
                    print("must be invalid message format")
                except Exception as ex:
                    print("something happened... " + str(ex))
                    break
        
        return threading.Thread(target=thr_body, args=(self.pub_port, ))
        
        


    def get_sub_thread(self):
        print("sub handler")
        def thr_body(port):
            sock = zmq.Context().socket(zmq.REP)
            sock.bind("tcp://*:" + str(port))

            while True:
                print('waiting for sub ... ')
                try:
                    content = sock.recv_string()
                    print("raw msg from sub port:\n" + content)
                    content = content.split("#")
                    if content[0] == "REG":
                        pub_list = self.register_sub(content[2], content[1])
                        sock.send_string("DONE_REG#" + str(pub_list))
                    elif content[0] == "CCL":
                        self.cancel_sub(content[2], content[1])
                        sock.send_string("DONE_CCL")
                except IndexError:
                    print("must be invalid message format")
                except Exception as ex:
                    print("something happened... " + str(ex))
                    # break
        
        return threading.Thread(target=thr_body, args=(self.sub_port, ))

    def start(self):
        pub_thr = self.get_pub_thread()
        sub_thr = self.get_sub_thread()
        global_thr.append(pub_thr)
        global_thr.append(sub_thr)


        pub_thr.start()
        sub_thr.start()

        pub_thr.join()
        sub_thr.join()


def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument ("-p", "--pub", type=int, default=5557, help="broker's sub port")
    parser.add_argument ("-s", "--sub", type=int, default=5557, help="broker's pub port")

    
    # parse the args
    args = parser.parse_args ()

    return args


if __name__ == "__main__":
    args = parseCmdLineArgs()
    broker = Broker(args.pub, args.sub)
    broker.start()


        