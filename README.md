# rabbitMqConnector

RabbitMq connector class is a wrapper client which handles message passing between rabbitMq Broker.

# Features
1. Sync consuming - Messages are received one by one through the receiver polling to get message<br>
2. Async consuming- Messages are received asynchronously as soon as message is transffered to Broker Queue<br>
3. Veda Labs resource Subscriptions- Async messages but in form of resource subscriptions<br>
4. Publishing Message- Send message to the broker on a specific topic<br>

** Note that this client uses only message exchange type "topic".<br>

# Usage
```
# Import rabbitMq connector module
import rabbitMqConnector as Connector

# Build the config params
RABBIT_SERVER_CONFIG={
    'host':"queue.vedalabs.in",
    'user':'***',
    'password':'**',
    'port':15672
}
REST_API_CONFIG={
    'VEDA_USER':'****.vedalabs.in',
    'VEDA_PASSWORD':'***',
    'VEDA_SERVER_URL':'https://api.staging.vedalabs.in',
    'VEDA_API_VERSION':'v1/rest'
}

# callback class for async consumer topic
def topiccallBack(message,props=None,methods=None):
  print("message received from topic callback",message)

# callback for subscriptions
def subscriptionCallback(message,props=None,methods=None):
  print("message received from subscription callback",message)
    
# Instantiate the connector class
client=Connector.RabbitMqConnector(rabbit_server_config=RABBIT_SERVER_CONFIG,
                                   topicCallback=topicCallBack,
                                   subscriptionCallback=subscriptionCallback,
                                   consumerTopics=None,
                                   consumerSyncTopics=["depth_camera_face_behaviour"],
                                   consumerSubscriptions=[{
                                        "behaviour":'5fe470510daa34ff247fb969'} ],
                                   producerTopic="cool",
                                   rest_api_config=REST_API_CONFIG,
                                   sender_exchange="BEHAVIOUR_EVENTS",
                                   receiver_exchange="BEHAVIOUR_EVENTS",
                                   queueId=500,
                                   sender_rabbit_server_config=None,
                                   receiver_rabbit_server_config=None
                                   )
# Send some message
client.send(producerTopic="hello",message={"message":"i am hello"})

# Receive sync messages every 0.01 sec
while(True):
  message=client.consume_sync("depth_camera_face_behaviour")
  time.sleep(0.01)

```
