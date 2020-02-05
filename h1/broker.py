#!/usr/bin/python
# encoding: utf-8
# Borker has two port to handle the pub and sub
# xsub port is 5556
# xpub port is 5557

import zmq
import time
import argparse
from multiprocessing import Process
from broker_lib import broker_lib

log_file = './Output/broker.log'

def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument('-a', '--address', type=str, help='Ip address of broker.')
    # parse the args
    args = parser.parse_args ()

    return args

#def sub():



def main():
    
    args = parseCmdLineArgs()
    broker_ip = args.address

    # deploy the port number for the coming request from pubs and subs
    xpub = '5555'
    xsub = '5556'
    # we init the broker library to store the message we receive 
    broker = broker_lib(xpub, xsub, broker_ip)
    
    pubsocket = broker.pubsocket
    print("Bound to port 5555 and waiting for any publisher to contact\n")
    subsocket = broker.subsocket
    print("Bound to port 5556 and waiting for any subscriber to contact\n")
    

    # we first register all the pub and sub
    msg1 = read(pubsocket.recv_string())
    dict_update(msg1,broker.pubdict)
    

    #pubsocket.send_string("Please send me the publisher \n")

    #msg2 = read(subsocket.recv_string)
    #dict_update(msg2,broker.subdict)
    

    poller=zmq.Poller()
    poller.register(pubsocket, zmq.POLLIN)
    poller.register(subsocket, zmq.POLLIN)
    
    while True:
        
        events=dict(poller.poll())
        
        if pubsocket in events:
            breakpoint()
            # how we deal with the coming publishers
            msg=read(pubsocket.recv_string())
            # add the coming info to the dict
            dict_update(msg, broker.pubdict)
            # reply to pub that this process is done
            pubsocket.send('Done!!!')
        
        if subsocket in events:

            topic = subsocket.recv()
            msg_body = find(topic, broker.pubdict)


            subsocket.send_multipart([topic, msg_body])

def read(msg):
    info = msg.split('#')
    return info

def find(topic, dict):

    content = dict[topic]

    # when we receive the request from subs, we look up the dict to find whether there exits the info
    

    return content
    

def dict_update(msg, dict):
    
    if msg[0] == 'init':
        pubID = msg[1]
        topic = msg[2]
        try:
            dict.update({topic:[]})
            with open(log_file, 'a') as logfile:
                logfile.write('Init msg: %s init with topic %s\n' % (pubID, topic))
        except KeyError:
            pass

    elif msg[0] == 'publish':
        pubID = msg[1]
        topic = msg[2]
        publish = msg[3]

        try:
            dict[topic].update([publish])
            with open(log_file, 'a') as logfile:
                logfile.write('Publication: %s published %s with topic %s\n' % (pubID, publish, topic))
        except KeyError:
            pass
    '''
    elif msg[0] == 'ask':
        se = 1
    '''

if __name__=="__main__":
	
    main()


