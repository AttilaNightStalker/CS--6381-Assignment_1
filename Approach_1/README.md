## Approach 1 for the sub-pub communication
### Desciption
Broker will register the information of subcribers and publishers. 
Broker binds two ports that listen to the requests from publishers and subscribers respectively. 

When a publisher sends a register topic request, the broker will update the publish-topic maps and notify the subscribers who is subscribing this the topic. 

When a subscriber sends a register topic request, the broker will reply the subscriber with a list of publisher's ip and port that publish such topic. 

The broker will not proxy the topic data. Subscribers always listen to and subscribe the publisher according to broker's information.

### How to run the code

run code with:

`python3 mn.py -p <number of publishers> -s <number of subscribers>`

Then the average message time delay will be calculated after publisher have published 60 posts