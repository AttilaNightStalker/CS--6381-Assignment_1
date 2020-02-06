#!/usr/bin/python
# encoding: utf-8

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
        self.content = []

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

    print("***** register_sub *****")

    connection = "tcp://" + baddress + ":5556"
    

    # publisher requests to the broker 
    current = time.time()
    while (time.time() - current < 5):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        socket.connect(connection)
	
    if socket is None:
        print('Connecttion failed.')
        
    else:
        print('Connecttion succeed.')
        
        message = 'reg' + '#' + id + '#' 
       
        # send the message
        
        socket.send_string( message )
        
        recv_msg = socket.recv_string()
        
        print(recv_msg)


        socket.close()
        

       

	# registation finished

def connection(s,t,id, file,content):
    
    current_time = time.time()
    topic = t
    print("Receiving messages on topics: %s ..." % topic)

    message = 'ask' + '#' + id + '#' + t + '#'
        
    # send the message
    time.sleep(1)
   
    

    s.send_string( message )
    #s.setsockopt_string(zmq.SUBSCRIBE,topic)
  
    try:
        while True:

            recv_msg = s.recv_string()
            #topic, msg = s.recv_multipart()
            if (recv_msg == 'Nothing')| (recv_msg == 'Connected'):
                if time.time()-current_time < 10:
                    time.sleep(1)
                    print('still waiting for the message')
                    m = 'ask' + '#' + id + '#' + t + '#'
                    s.send_string(m)
                else:
                    break
            else:
                print('Topic: %s, msg:%s' % (topic, recv_msg))
                if recv_msg not in content:
                    content.append(recv_msg)
                
                    with open(file, 'a') as log:
                        log.write('Receive from broker' + ' \n')
                        log.write('Topic: ' + topic + '\n') 
                        log.write('Content:' + recv_msg+ '\n')
                
                    m = 'ask' + '#' + id + '#' + t + '#'
                    s.send_string(m)
                else:
                    #end = 'end' + '#' + id + '#' 
                    #s.send_string(end)
                    s.close()
                    break

            
    except KeyboardInterrupt:
        end = 'end' + '#' + id + '#' 
        s.send_string(end)
        s.close()
    print("Done.")
            

def main():
    
    args = parseCmdLineArgs()
    
    baddress = args.address
    port = '5556'

    topics = {1:'animals', 2:'countries', 3:'foods', 4:'laptops', 5:'phones', 6:'universities'}
    topic = topics[random.randint(1, 6)]
    #topic = topics[1]

    sub = Sub_Info(baddress, port, topic)
    register_sub(sub.port, sub.topic, sub.address, sub.ID)
    time.sleep(5)

    context = zmq.Context()
    
    subsocket = context.socket(zmq.REQ)
    
    current = time.time()
    
    subsocket.connect("tcp://" + baddress + ":" + port)
    #subsocket.bind('tcp://*:'+ '1111')
    
    
    
    sub_logfile = './Output/' + sub.ID + '-subscriber.log'
    
    with open(sub_logfile, 'w') as log:
        log.write('ID: ' + sub.ID + '\n')
        log.write('Topic: ' + sub.topic + '\n') 
        log.write('Connection: tcp://%s:%s\n' % (sub.address,sub.port))

    
    
    # we subscribe the topic the subs need
    #socket.subscribe(sub.topic)

    
    
    
    # we need to make it alive to receive the message from broker
    if subsocket is None:
        print('Connection failed.')
        #return False
    else:
        print('Connection succeed!')
        threading.Thread(target=connection(subsocket,sub.topic,sub.ID,sub_logfile,sub.content), args=()).start()
    
    # wait for the sub to be registered
    
    time.sleep(5)


    

if __name__ == '__main__':

    main()