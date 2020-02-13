#!/usr/bin/python
import os              # OS level utilities
import sys
import argparse   # for command line parsing

from signal import SIGINT

import time
import threading
import subprocess

# These are all Mininet-specific
from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.net import CLI
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel, info
from mininet.util import pmonitor
from mininet.node import Controller, RemoteController
# This is our topology class created specially for Mininet
from Stopology import Stopo

def parseCmdLineArgs ():
    # parse the command line
    parser = argparse.ArgumentParser ()

    # add optional arguments
    parser.add_argument ("-p", "--pub", type=int, default=3, help="Number of publishers, default 3")
    parser.add_argument ("-s", "--sub", type=int, default=5, help="Number of subscriber, default 5")
    
    # parse the args
    args = parser.parse_args ()

    return args


def Test(pubHosts, subHosts, brokerHost):
    try:
        broker_ip = brokerHost.IP()

        def broker_op():
            # Invoke broker and wait for the broker to be completed
            command = 'xterm -e python3 broker.py -a' + broker_ip
            brokerHost.cmd(command)

        threading.Thread(target=broker_op, args=()).start()

        print ('broker is ready')

        time.sleep(5)

        for pub in pubHosts:
            
            def pub_op():
                command1 = 'xterm -e python3 publisher.py -a ' + broker_ip
                pub.cmd(command1)
            
            threading.Thread(target=pub_op, args=()).start()
            
            time.sleep(5)
        
        print ('Pubs are ready')
        for sub in subHosts:
            
            def sub_op():
                command2 = 'xterm -e python3 subscriber.py -a ' + broker_ip
                sub.cmd(command2)
            
            threading.Thread(target=sub_op, args=()).start()
            time.sleep(5)
        
        print ('Subs are ready')

        while True:
            pass


    except Exception as e:
        print(e)

def main ():
    
    "Create and run the Wordcount mapreduce program in Mininet topology"
    parsed_args = parseCmdLineArgs ()
    
    # instantiate our topology
    print ('Instantiate topology')
    stopo = Stopo(pub = parsed_args.pub, sub = parsed_args.sub)

    # create the network
    print ('Instantiate network')
    net = Mininet (topo=stopo, link=TCLink)
    
    # activate the network
    print ('Activate network')
    net.start()

    # debugging purposes
    print ('Dumping host connections')
    dumpNodeConnections (net.hosts)
    
    
    
    #net.pingAll ()


    pubhosts =[]
    subhosts = []
    brokerhost = None
    
    for host in net.hosts:
        if 'PUB' in host.name:
            pubhosts.append(host)
        elif 'SUB' in host.name:
            subhosts.append(host)
        elif 'Broker' in host.name:
            brokerhost = host

    Test(pubhosts, subhosts, brokerhost)
   
    net.stop ()

if __name__ == '__main__':
    
    main ()
