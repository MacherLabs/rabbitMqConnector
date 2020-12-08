import rabbitMqConnector as Connector

def callback(msg):
    print(msg)
    
client=Connector.RabbitMqConnector(host="queue.vedalabs.in",callback=callback,consumerTopics=["mask"],producerSubscriptions={"device":"7d9c247d-ebf2-44bd-a851-b64521107d84","topic":"commands"})

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
client.send(message=message)
