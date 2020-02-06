from publisher import Publisher
from time import sleep
from random import randint

my_pub = Publisher("10.0.0.1", 5556)

my_pub.register_pub("topic1")
my_pub.register_pub("topic2")
my_pub.register_pub("topic3")

while True:
    curtopic = "topic" + str(randint(1, 3))
    my_pub.publish("topic" + str(randint(1, 3)), str(randint(100000, 999999)))
    sleep(1)