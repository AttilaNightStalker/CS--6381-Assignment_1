#!/usr/bin/python
# encoding: utf-8
# Borker has two port to handle the pub and sub
# xsub port is 5556
# xpub port is 5557

import zmq
import time
import random
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
    dict_update(msg1,broker.pubdict, broker.publisher)
    pubsocket.send_string('PUB-Info has been received!')
    

    #pubsocket.send_string("Please send me the publisher \n")

    msg2 = read(subsocket.recv_string())
    sdict_update(msg2,broker.subdict, broker.subscriber)
    subsocket.send_string('SUB-BROKER has been connected')
    

    poller=zmq.Poller()
    poller.register(pubsocket, zmq.POLLIN)
    poller.register(subsocket, zmq.POLLIN)
    
    while True:
        
        events=dict(poller.poll())
        
        if pubsocket in events:
            # how we deal with the coming publishers
            msg=read(pubsocket.recv_string())
            # add the coming info to the dict
            dict_update(msg, broker.pubdict, broker.publisher)
            # reply to pub that this process is done
            pubsocket.send_string('Done!!!')
        
        if subsocket in events:
            

            msg = read(subsocket.recv_string())
            sdict_update(msg, broker.subdict, broker.subscriber)
            if msg[0] == 'ask':
                pub_msg = find(msg[2],broker.pubdict,broker.publisher)
                subsocket.send_string(pub_msg)
            elif msg[0] == 'end':
                time.sleep(1)
                subsocket.send_string('END!!')
            elif msg[0] == 'reg':
                subsocket.send_string('Connected')
                


def read(msg):
    info = msg.split('#')
    return info

def find(topic, dict, publisher):

    
    
    
    # we pick the first one to PUB
    if topic in publisher:
        owner = publisher[topic]
        length = len(dict[owner[0]][topic] )
        i = random.randint(1, length)
        content = dict[owner[0]][topic][i-1]
    else:
        content = 'Nothing'
    
    

    # when we receive the request from subs, we look up the dict to find whether there exits the info
    

    return content
    

def dict_update(msg, pubdict, publisher):
    
    if msg[0] == 'init':
        pubID = msg[1]
        topic = msg[2]
        try:
            #dict.update(topic)
            pubdict.update({pubID: {topic:[]}})
            publisher.update({topic:[]})
            #publisher[topic].update([pubID])

            with open(log_file, 'a') as logfile:
                logfile.write('PUB-Init msg: %s init with topic %s\n' % (pubID, topic))
        except KeyError:
            pass

    elif msg[0] == 'publish':
        pubID = msg[1]
        topic = msg[2]
        publish = msg[3]

        try:
            #dict[topic].update([publish])
            pubdict[pubID][topic].append(publish)
            if pubID not in publisher[topic]:
                publisher[topic].append(pubID)

            with open(log_file, 'a') as logfile:
                logfile.write('Pub No.%s published %s with topic %s\n' % (pubID, publish, topic))
        except KeyError:
            pass
def sdict_update(msg, dict, sub):
    
    if msg[0] == 'reg':
        subID = msg[1]
        dict.update({subID: []})
        #sub[topic].append(subID)
    elif msg[0] == 'ask':
        subID = msg[1]
        topic = msg[2]
        dict[subID].append(topic)
        #sub[topic].append(subID)
                
    elif msg[0] == 'end':
        subID = msg[1]
        #sub[topic].remove(subID)
        dict.pop(subID)
    
    
    

if __name__=="__main__":
	
    main()


