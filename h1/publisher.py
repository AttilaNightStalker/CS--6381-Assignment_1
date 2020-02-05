#!/usr/bin/python
# encoding: utf-8
import os              # OS level utilities
import sys
import argparse   # for command line parsing

import random
import time
import threading
import zmq
from multiprocessing import Process

class Pub_Info:

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


def register_pub(no, topic, baddress,id):

    print('Publisher NO. %s with %s.' % (id, topic))


    connection = "tcp://" + baddress + ":5555"
    context = zmq.Context()

    # publisher requests to the broker 
    the_socket = context.socket(zmq.REQ)
	
    current = time.time()

    # publisher send a socket to the 
    while (time.time() - current < 5):
        the_socket.connect(connection)

    if the_socket is None:
        print('Connection failed.')
        #return False
    else:
        print('Connection succeed!')

        message = 'init' + '#' + id + '#' + topic + '#'
        # send the message
        
        the_socket.send_string( message )
        
        #return True

	# registation finished
    the_socket.close()



def get_publications(file_path):
	try:
		with open(file_path, 'r') as file:
			pubs = file.readlines()
		for i in range(len(pubs)):
			pubs[i] = pubs[i][:-1]
		return pubs
	except IOError:
		print('Open or write file error.')
		return []

def main():
    
    

    args = parseCmdLineArgs()
    
    baddress = args.address

    # we define all the topics we have in this section
    topics = {1:'animals', 2:'countries', 3:'foods', 4:'countries', 5:'phones', 6:'universities'}
    
    # select the topic randomly
    topic = topics[random.randint(1, 6)]
    
    # we first init the publish server
    pub = Pub_Info(baddress,'5555',topic)

    # we register the publisher to the broker with port_number, topic, the broker address and its id
    register_pub(pub.port, pub.topic, pub.address, pub.ID)

    # wait for the registation complete
    time.sleep(5)
    
    # we find the path for the topic
    path = './Input/'+ topic + '.txt'
    

    list = get_publications(path)
    

    print('PUB ID:', pub.ID)

    # we find the file from path
    file = './Output/' + pub.ID + '-publisher.log'
    
    context = zmq.Context()
    
    pubsocket = context.socket(zmq.REQ)
    
    current = time.time()
    
    pubsocket.connect("tcp://" + baddress + ":5555")
    
    Process(target= publish(pub, pub.ID, topic, file, list, pubsocket)).start()
    
    #wait()

def publish(pub, id, topic, file, list, pub_s):
    try:
        with open(file, 'a') as logfile:
            for pub in list:
                logfile.write('*************************************************\n')
                
                logfile.write('Publish Info: %s \n'% topic)
                logfile.write('Publish: %s\n' % pub)
                logfile.write('Time: %s\n' % str(time.time()))
                send = 'publish' + '#' + id + '#' + topic + '#' + pub
                pub_s.send_string(send)
                time.sleep(1)
            pub_s.close()
    except IOError:
        print('Open or write file error.')


if __name__ == '__main__':

    main()
    
    
    