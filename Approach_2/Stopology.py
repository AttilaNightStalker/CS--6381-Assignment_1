from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink

class Stopo(Topo):
    def build(self, pub, sub):
        self.pubHosts = []
        self.subHosts = []
        self.brokerHost = None
        self.switch = None

        print('Topology Architecture: Star Topology')
        
        # Add a switch
        self.switch = self.addSwitch('s1')
        print('Add a switch.')

        # Add broker host
        self.brokerHost = self.addHost('Broker')
        print('Add Broker host')

        # Add link between switch and broker host
        self.addLink(self.brokerHost, self.switch)
        print('Add link between switch and broker host')

        # Add publisher hosts to the switch
        for i in range(pub):
            host = self.addHost('PUB%d' % (i+1))
            self.pubHosts.append(host)
            
            self.addLink(self.pubHosts[i], self.switch)
            print('Add link between publisher host ' + self.pubHosts[i] + ' and switch ' + self.switch)

        # Add subscriber hosts to the switch
        for i in range(sub):
            host = self.addHost('SUB%d' % (i+1))
            self.subHosts.append(host)
            
            self.addLink(self.subHosts[i], self.switch)
            print('Add link between subscriber host ' + self.subHosts[i] + ' and switch ' + self.switch)
