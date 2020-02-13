# CS--6381-Assignment_1

In this assignment we provide two solutions for pub-sub communication as it states in the description:

1)
Have the publisher’s middleware layer directly send the data to the subscribers who are interested in the topic being published by this publisher. This means that,as the number of subscribers for this topic increases, the publisher has to send data toeach of the subscribers.

2)
Have the publisher’s middleware always send the information to the broker, which then sends it to the subscribers for this topic. Such an approach saves the amount of data sent by the Application LogicCS6381 APIUnderlying ZMQ publisher but clearly can make the broker a bottleneck as the number of publishers and subscribers in the system increases.

Leqiang implements 1) and Shuang is responsible for 2). Two approaches are in seperated folders with its own readme for details. 
