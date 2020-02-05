#!/usr/bin/python
import zmq

log_file = './Output/broker.log'

class broker_lib:
    def __init__(self, xpub, xsub, ip):
        self.pubsocket = self.bindp(ip, xpub)
        self.subsocket = self.binds(ip, xsub)
        self.pubdict = {}
        self.subdict = {}
    
    print('\n************************************\n')
    print('Init MyBroker succeed.')
    print('\n************************************\n')
    
    with open(log_file, 'w') as logfile:
        logfile.write('Init Broker succeed \n')
    
    def bindp(self,ip, port):
        # we use REQ and REP to manage between publisher and broker
        context = zmq.Context()
        p_socket = context.socket(zmq.REP)
        p_socket.bind('tcp://*:'+ port)
        with open(log_file, 'a') as logfile:
            logfile.write('5555! \n')
        return p_socket
    
    def binds(self, ip, port):
        # we use PUB and SUB to manage sub and broker
        context = zmq.Context()
        s_socket = context.socket(zmq.PUB)
        s_socket.bind('tcp://*:' + port)
        with open(log_file, 'a') as logfile:
            logfile.write('5556! \n')
        
        return s_socket


