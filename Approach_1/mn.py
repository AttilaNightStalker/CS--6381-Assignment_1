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
    parser.add_argument ("-sp", "--sub_port", type=int, default=5557, help="sub port for broker")
    parser.add_argument ("-pp", "--pub_port", type=int, default=5556, help="pub port for broker")
    # parse the args
    args = parser.parse_args ()

    return args


def collectResult():
    logFiles = [f for f in os.listdir("logs/") if os.path.isfile(os.path.join("logs/", f))]

    print(logFiles)

    subLogs = list(filter(lambda s: "sub_" in s, logFiles))
    pubLogs = list(filter(lambda s: "pub_" in s, logFiles))

    loggedTime = {}

    for f in pubLogs:
        fp = open("logs/" + f, "r")
        logs = fp.read().split("\n")[:-1]
        for l in logs:
            print(l)
            l = l.split(",")
            loggedTime[l[0]] = eval(l[1])
        fp.close()
    
    cnt = 0
    sum_delay = 0.0    
    for f in subLogs:
        fp = open("logs/" + f, "r")
        logs = fp.read().split("\n")[:-1]
        for l in logs:
            l = l.split(",")
            try:
                sum_delay += eval(l[1]) - loggedTime[l[0]]
                cnt += 1
            except Exception as ex:
                print("something wrong: " + str(ex))
    
    print("avrg delay:\n" + str(sum_delay/cnt))


def Test(pubHosts, subHosts, brokerHost, subPort, pubPort):
    pubthrList = []
    subthrList = []
    number = 10
    wait_step = 20

    os.system("rm -rf logs/*")
    try:
        broker_ip = brokerHost.IP()
        
        def broker_op():
            # Invoke broker and wait for the broker to be completed
            command = 'xterm -e python3 broker.py -p ' + str(pubPort) + ' -s ' + str(subPort)
            brokerHost.cmd(command)
        thr = threading.Thread(target=broker_op, args=())
        # thrList.append(thr)
        thr.start()

        print ('broker is ready')

        time.sleep(1)


        for sub in subHosts:
            
            def sub_op():
                command2 = 'xterm -e python3 subscriber.py -b ' + broker_ip + ' -p ' + str(subPort)
                sub.cmd(command2)
            
            thr = threading.Thread(target=sub_op, args=())
            subthrList.append(thr)
        
            thr.start()
            print("sub thread started ... ")
            time.sleep(0.5)
        
        print ('Subs are ready')

        for pub in pubHosts:
            
            def pub_op():
                command1 = 'xterm -e python3 publisher.py -b ' + broker_ip + ' -p ' + str(pubPort)
                pub.cmd(command1)
            
            thr = threading.Thread(target=pub_op, args=())
            pubthrList.append(thr)

            thr.start()
            print("pub thread started ... ")
            time.sleep(0.5)
        
        print ('Pubs are ready')

        # while True:
        #     pass

        # for i in range(wait_step):
        #     print("remaining steps " + str(wait_step - i))
        #     time.sleep(2)


    except Exception as e:
        print(e)
    
    return pubthrList

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
    
    
    
    net.pingAll ()


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

    thrList = Test(pubhosts, subhosts, brokerhost, parsed_args.sub_port, parsed_args.pub_port)

    # CLI(net)
    print("joining")
    for thr in thrList:
        thr.join()

    print("joined")

    collectResult()

if __name__ == '__main__':
    
    main ()
