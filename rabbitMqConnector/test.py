import rabbitMqConnector as Connector
import time


    
class test_class():
  
  def _init__(self):
    self.a=0
    
     
RABBIT_SERVER_CONFIG={
    'host':"queue.vedalabs.in",
    'user':'guest',
    'password':'guest',
    'port':15672
}

REST_API_CONFIG={
    'VEDA_USER':'saurabhk6@vedalabs.in',
    'VEDA_PASSWORD':'password',
    'VEDA_SERVER_URL':'https://api.staging.vedalabs.in',
    'VEDA_API_VERSION':'v1/rest'
}
    
# client=Connector.RabbitMqConnector(rabbit_server_config=RABBIT_SERVER_CONFIG,
#                                    consumerTopics=["depth_camera_face_behaviour"],
#                                    consumerSyncTopics=["depth_camera_face_behaviour"],
#                                    consumerSubscriptions=None,
#                                    producerTopic="cool",
#                                    rest_api_config=REST_API_CONFIG,
#                                    sender_exchange="BEHAVIOUR_EVENTS",
#                                    receiver_exchange="BEHAVIOUR_EVENTS",queueId=500)

client=Connector.RabbitMqConnector(rabbit_server_config=RABBIT_SERVER_CONFIG,
                                   consumerTopics=None,
                                   consumerSyncTopics=['depth_camera_face_behaviour'],
                                   consumerSubscriptions=None,
                                   producerTopic="cool",
                                   rest_api_config=REST_API_CONFIG,
                                   sender_exchange="BEHAVIOUR_EVENTS",
                                   receiver_exchange="BEHAVIOUR_EVENTS",queueId=500,
                                   sender_rabbit_server_config=None,
                                   receiver_rabbit_server_config=None
                                   )

message={
  "from" : "userid of sender",
  "to" : "deviceid",
  "type" : "command",
  "commandContext" : {
    "corelationId": "1234567",
    "organization" : "123456",
    "hub": "12345",
    "device" : "4567",
    "expectedResponseContentType" : "application/json",
    "showProgress" : True,
    "timestamp": "1234"
  },
  "command": {
    "name" : "appdeploy",
    "commandVars": {
      "myvar1": "myvalue1"
    },
    "commandArgs": [
      {
        "arg1": "value1"
      }
    ]
  }
}
time.sleep(1)
client.send(message=message)

print("message:",message)
#client.flush_sync_consumer_queues()
client.send(producerTopic="a",message={"message":"i am a","source_id":10})
#message=client.consume_sync_all()
print("message received:",message)
time.sleep(1)
client.send(producerTopic="a",message={"message":"i am a1","source_id":20})
client.send(producerTopic="b",message={"message":"i am b1","source_id":22})
client.send(producerTopic="hello",message={"message":"i am hello"})

print('********************************************************************************')
test_obj = test_class()
if hasattr(test_obj, 'client'):
  print("motherfucker")
test_obj.client=client

if hasattr(test_obj, 'client'):
  print("fuckfucker")
  test_obj.client.add_subscriptions(subscriptions=[{
                    "behaviour":'5fc75e3e47074021c36f946b'
                }])
#message=client.consume_sync_all()


  #print("message received:",message)
time.sleep(10)
test_obj.client.remove_subscriptions(subscriptions=[{
                    "behaviour":'5fc75e3e47074021c36f946b'
                } ])
#client.stop()

    