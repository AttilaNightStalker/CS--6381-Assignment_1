#!/usr/bin/python
import os              # OS level utilities
import sys
import argparse   # for command line parsing

import random
import time
import threading
import zmq

class Sub_Info:

    # we define the publisher with borker_address, port and the topic it has
    
    def __init__(self, address, port, topic):

        self.address = address
        self.port = port
        self.topic = topic

        # we randomly select the id for this publisher
        self.ID = str(random.randint(1, 10))
        


def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument('-a', '--address', type=str, help='Please enter ip address of broker.')
    # parse the args
    args = parser.parse_args ()

    return args

def register_sub(no, topic, baddress,id):

    connection = "tcp://" + baddress + ":5556"
    

    # publisher requests to the broker 
    current = time.time()
    while (time.time() - current < 5):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        socket.connect(connection)
	
    if socket is None:
        print('Connecttion failed.')
        
    else:
        print('Connecttion succeed.')

	# registation finished
    return socket

def connection(s,t):
    topic = t
    print("Receiving messages on topics: %s ..." % topic)
    s.setsockopt_string(zmq.SUBSCRIBE,topic)
  
    try:
        while True:
            topic, msg = s.recv_multipart()
            print('Topic: %s, msg:%s' % (topic, msg))
    except KeyboardInterrupt:
        pass
    print("Done.")
            

def main():
    
    args = parseCmdLineArgs()
    
    baddress = args.address
    port = '5556'

    topics = {1:'animals', 2:'countries', 3:'foods', 4:'laptops', 5:'phones', 6:'universities'}
    topic = topics[random.randint(1, 6)]

    sub = Sub_Info(baddress, port, topic)
    
    socket = register_sub(sub.port, sub.topic, sub.address, sub.ID)
    
    # wait for the sub to be registered
    time.sleep(5)
    
    
    sub_logfile = './Output/' + sub.ID + '-subscriber.log'
    
    with open(sub_logfile, 'w') as log:
        log.write('ID: ' + sub.ID + '\n')
        log.write('Topic: ' + sub.topic + '\n') 
        log.write('Connection: tcp://%s:%s\n' % (sub.address,sub.port))
    
    # we subscribe the topic the subs need
    #socket.subscribe(sub.topic)

    
    
    
    # we need to make it alive to receive the message from broker
    connection(socket,topic)


    

if __name__ == '__main__':

    main()