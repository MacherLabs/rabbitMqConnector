import rabbitMqConnector as Connector
import time


    
    
RABBIT_SERVER_CONFIG={
    'host':"queue.vedalabs.in",
    'user':'guest',
    'password':'guest',
    'port':5672
}

REST_API_CONFIG={
    'VEDA_USER':'saurabhk6@vedalabs.in',
    'VEDA_PASSWORD':'password',
    'VEDA_SERVER_URL':'http://localhost:8001',
    'VEDA_API_VERSION':'v1/rest'
}
    
client=Connector.RabbitMqConnector(host="queue.vedalabs.in",consumerSyncTopics=["a","b"],producerTopic="cool",rabbit_server_config=RABBIT_SERVER_CONFIG,rest_api_config=REST_API_CONFIG,sender_exchange="BEHAVIOUR_EVENTS",receiver_exchange="BEHAVIOUR_EVENTS",queueId=146)

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
time.sleep(5)
client.send(message=message)

print("message:",message)
client.flush_sync_consumer_queues()
client.send(producerTopic="a",message={"message":"i am a","source_id":10})
message=client.consume_sync_all()
print("message received:",message)
time.sleep(1)
client.send(producerTopic="a",message={"message":"i am a1","source_id":20})
client.send(producerTopic="b",message={"message":"i am b1","source_id":22})
client.send(producerTopic="hello",message={"message":"i am hello"})
message=client.consume_sync_all()

print("message received:",message)

    