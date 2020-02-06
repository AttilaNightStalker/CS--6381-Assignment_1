from subscriber import Subscriber

my_sub = Subscriber("10.0.0.1", 5557)
my_sub.start()

my_sub.register_sub("topic1")
my_sub.register_sub("topic2")