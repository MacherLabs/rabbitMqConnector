import logging
logging.basicConfig()
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.info("Loaded " + __name__)

import pika
import threading
import datetime,time
import json
from anytree import Node, search
from anytree.exporter import DotExporter
import requests
from requests.auth import HTTPBasicAuth

def callback(message):
    logger.info("message received finally-{}".format(message))
        
class RabbitMqConnector():
    
    def __init__(self,host,callback=callback,consumerSubscriptions=None,consumerTopics=None,producerSubscriptions=None,producerTopic=None,**kwargs):
       
        rabbit_server_config=kwargs.get("rabbit_server_config",{
        'host':host,
        'user':'guest',
        'password':'guest',
        'port':5672
        })
        sender_properties={
        "exchange":kwargs.get("sender_exchange","RESOURCES_UPDATES"),
        "exchange_type":"topic",
        "passive":False,
        "durable":True,
        "auto_delete":False,
        "subscription":producerSubscriptions,
        "producerTopic":producerTopic
        }
        receiver_properties={
        "exchange":kwargs.get("receiver_exchange","RESOURCES_UPDATES"),
        "exchange_type":"topic",
        "passive":False,
        "durable":True,
        "auto_delete":False,
        "subscriptions":consumerSubscriptions,
        "queue":"standard",
        "consumerTopics":consumerTopics
        }
        rest_api_config=kwargs.get("rest_api_config",{})
        if (producerSubscriptions is not None or consumerSubscriptions is not None) and rest_api_config=={}:
            logger.error("rest_api_config is mandatory for susbscriptions")
            return 
       
        try:
            logger.info("rabbit server config-{}, rest_api_config-{} ,sender_properties-{} ,receiver_properties-{}".format(rabbit_server_config,rest_api_config,sender_properties,receiver_properties))
            self.rabbit_server_config=rabbit_server_config
            self.creds=pika.PlainCredentials(rabbit_server_config['user'], rabbit_server_config['password'])
            self.heartbeat=31
            self.start = True 
            self.subRouteMap={}
            self.consume_failure=0
            self.sender_properties=sender_properties
            self.receiver_properties=receiver_properties
            self.rest_api_config=rest_api_config
            self.create_hierarchy()
            self.init_sender_connection()
            self.init_receiver_connection()
            self.callback=callback
            self.check_connection_thread.start()
            
        except Exception as e:
            logger.error("some error occurred initializing connection -{}".format(str(e)))
            
        
        
    def init_sender_connection(self):
        self.sender_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbit_server_config['host'],credentials=self.creds,heartbeat=self.heartbeat))
        self.sender_channel=self.sender_connection.channel()
        self.sender_channel.exchange_declare(
            exchange=self.sender_properties["exchange"],
            exchange_type=self.sender_properties["exchange_type"],
            passive=self.sender_properties["passive"],
            durable=self.sender_properties["durable"],
            auto_delete=self.sender_properties["auto_delete"])
        
    
    def init_receiver_connection(self):
        self.receiver_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbit_server_config['host'],credentials=self.creds,heartbeat=self.heartbeat))
        self.receiver_channel=self.receiver_connection.channel()
        self.receiver_channel.exchange_declare(
            exchange=self.receiver_properties["exchange"],
            exchange_type=self.receiver_properties["exchange_type"],
            passive=self.receiver_properties["passive"],
            durable=self.receiver_properties["durable"],
            auto_delete=self.receiver_properties["auto_delete"])
        receiver_queue=self.receiver_properties.get("queue","standard") 
        consumerTopics=self.receiver_properties.get("consumerTopics",None)
        subscriptions=self.receiver_properties.get("subscriptions",None)
        routingKeys=[]
        if consumerTopics is not None:
            for topic in consumerTopics:
                routingKeys.append(topic)
    
        if subscriptions is not None:
            for subscription in subscriptions:
                routingKey=self.getRoutingKey(subscription)
                routingKeys.append(routingKey)
                logger.info("subscribing to routes-{}".format(routingKey))
                #self.receiver_channel.queue_bind(queue=receiver_queue,exchange=self.receiver_properties["exchange"], routing_key=routingKey)
        
        receiver_queue='-'.join(routingKeys)
        self.receiver_channel.queue_declare(queue=receiver_queue)
        logger.info("subscribing to routes-{}, queue-{}".format(routingKeys,receiver_queue))
        routingKeys.append('test')
        for routingKey in routingKeys:
            self.receiver_channel.queue_bind(queue=receiver_queue,exchange=self.receiver_properties["exchange"], routing_key=routingKey)
            
                
        self.receiver_channel.basic_consume(queue=receiver_queue, on_message_callback=self.receive, auto_ack=True)
        self.consume_thread = threading.Thread(target=self.consume)
        self.check_connection_thread = threading.Thread(target=self.check_connection_state)
        self.consume_thread.start()
        
        
        
    def reinit_sender(self):
        logger.info("reinitializing sender connection")
        try:
            self.sender_connection.close()
            self.sender_channel.close()
        except Exception as e:
            logger.info("failed to reinitialize sender connection")
            pass
        self.init_sender_connection()
        
   
    def reinit_receiver(self):
        logger.info("reinitializing receiver connection")
        try:
            self.consume_thread.join()
            self.receiver_connection.close()
            self.receiver_channel.close()
        except Exception as e:
            logger.info("failed to reinitialize connection")
            pass
        
        self.init_receiver_connection()
        
    def check_connection_state(self):
        
        while(self.start):
            
            logger.info("checking connection state with server")
            
            self.send(producerTopic='test',message={"message":"ping---------------pong"})
                    
            if self.sender_connection.is_closed or self.sender_channel.is_closed:
                try:
                    self.reinit_sender()
                except Exception as e:
                    logger.info("failed to open sender connection..retrying")
                    time.sleep(self.heartbeat)
                    self.check_connection_state()
            
            if self.receiver_connection.is_closed or self.receiver_channel.is_closed:
                try:
                    self.reinit_receiver()
                except Exception as e:
                    logger.info("failed to open receiver connection..retrying")
                    time.sleep(self.heartbeat)
                    self.check_connection_state()
                
            time.sleep(self.heartbeat)
                
        
    def send(self,message={"message":"ping------pong"},subscription=None,producerTopic=None,showMessage=False):
        
        #import pdb;pdb.set_trace()
        if producerTopic is None:
            producerTopic=self.sender_properties["producerTopic"]
        if subscription is None:
            subscription=self.sender_properties["subscription"]
        
        
        if producerTopic is not None:
            routingKey=producerTopic
            
        elif subscription is not None:
            logger.info("sending to subscription-{}".format(subscription))
            routingKey=self.getRoutingKey(subscription)
    
        try:
            self.sender_channel.basic_publish(exchange=self.sender_properties["exchange"], routing_key=routingKey,body=json.dumps(message))
        except Exception as e:
            logger.info("failed to send message-{}".format(str(e)))
        if showMessage:
            logger.info("successfully sent message on routing key-{}, message-{}".format(routingKey,message))
        else:
            logger.info("successfully sent message on routing key-{}".format(routingKey))
            
        
    def receive(self,ch, method, properties, body):
        message=body.decode('utf8').replace("'", '"')
        try:
            message=json.loads(message)
        except Exception as e:
            logger.info("error loading message to json-{}".format(str(e)))
        routingKey=method.routing_key
        logger.info("message received from topic-{},body-{}".format(str(routingKey),str(message)))
        
        if routingKey != 'test':
            self.callback(message)
         
    def consume(self):
        logger.info("receiver started consuming")
        try:
            self.receiver_channel.start_consuming()
        except Exception as e:
            logger.info("some error occured while consuming-{}".format(str(e)))
            self.consume_failure +=1
            if self.consume_failure<5:
                self.consume()
            if self.start == True:
                self.reinit_receiver()
            
        
    def stop(self):
        self.consume_thread.join()
        
    def create_hierarchy(self):
        logger.info('resource hierarchy being setup!')
        Node.separator="."
        self.organizations = Node("organizations")
        self.hubs = Node('hubs',parent=self.organizations)
        self.behaviourTypes = Node('behaviourTypes',parent=self.organizations)
        self.users = Node('users',parent=self.organizations)
        self.artefacts = Node("artefacts",parent=self.organizations)
        self.notifications = Node("notificaitons",parent=self.organizations)
        self.pipelines = Node("pipelines",parent=self.organizations)
        self.charts = Node("charts",parent=self.organizations)
        self.recommendations = Node("recommendations",parent=self.organizations)
        self.cameras = Node("cameras",parent=self.hubs)
        self.devices = Node("devices",parent=self.hubs)
        self.behaviours = Node("behaviours",parent=self.cameras)
        self.alerts = Node("alerts",parent=self.behaviours)
        self.resourceMap={
                          "organization":"organizations",
                          "hub":"hubs",
                          "behaviourType":"behaviourTypes",
                          "user":"users",
                          "artefact":"artefacts",
                          "notificaiton":"notificaitons",
                          "pipeline":"pipelines",
                          "chart":"charts",
                          "recommendation":"recommendations",
                          "camera":"cameras",
                          "device":"devices",
                          "behaviour":"behaviours",
                          "alert":"alerts",
                         }
        
    def getRoutingKey(self,subscription):
        subRouteKeys=self.subRouteMap.keys()
        if str(subscription) in subRouteKeys:
            return self.subRouteMap[str(subscription)]
        sub_keys=subscription.keys()
        resourceKeys = self.resourceMap.keys()
        resourceKey=None
        for key in sub_keys:
            if key in resourceKeys:
                resourceKey=key
                break
        if resourceKey is not None:
            resource_id=subscription[resourceKey]
            resource = self.get_resource(self.resourceMap[resourceKey],resource_id)
            route= self.findRoute(self.resourceMap[resourceKey])
            routelist = route.split('.')
            #print("routelist",routelist)
            finalRoute=''
            for name in routelist:
                name=name[:-1]
                if name==resourceKey:
                    finalRoute+=str(resource['_id'])+'.'
                else:
                    finalRoute+=str(resource[name])+'.'
                    
            topic=subscription.get('topic',None)
            if topic is not None:
                finalRoute+=str(subscription['topic'])
            else:
                finalRoute=finalRoute[:-1]
        self.subRouteMap[str(subscription)] = finalRoute
        return finalRoute
        
    def findRoute(self,nodeName):
        anytreeNodePath = search.find(self.organizations, lambda node: node.name == nodeName)
        #print ("anytree node path:",anytreeNodePath)
        try:
            if anytreeNodePath:
                rt = str(anytreeNodePath).split('(')[1].replace("'.","",1).replace("')",'',1)
                #print ("designated route:",rt)
                return rt
        except Exception as anytreeError:
            #print ("anytree error occured",anytreeError)
            return None
    
    # Import app to get api_config
    def get_resource(self,res_type, res_id=None, res_filter=None):
        api_config=self.rest_api_config
        Veda_auth = HTTPBasicAuth(api_config['VEDA_USER'], api_config['VEDA_PASSWORD'])
        if res_id is not None:
            final_url = "{}/{}/{}/{}".format(api_config['VEDA_SERVER_URL'], api_config['VEDA_API_VERSION'], res_type, res_id)     
        elif res_filter is not None:
            final_url = "{}/{}/{}?where={}".format(api_config['VEDA_SERVER_URL'], api_config['VEDA_API_VERSION'], res_type, json.dumps(res_filter))
        else:
            final_url = "{}/{}/{}".format(api_config['VEDA_SERVER_URL'], api_config['VEDA_API_VERSION'], res_type)
        resp = {}
        try:
            items = []
            while True:
                logging.info("Fetching cloud resource: {}".format(final_url))
                resp = requests.get(final_url, auth=Veda_auth, timeout=10)
                resp.raise_for_status()
                resp = resp.json()
                if "next" in resp["_links"] and "href" in resp["_links"]["next"]:
                    route = resp["_links"]["next"]['href']
                    final_url = "{}/{}/{}".format(api_config['VEDA_SERVER_URL'], api_config['VEDA_API_VERSION'], route)
                    items = items + resp["_items"]
                else:
                    if items:
                        resp["_items"] = items
                    break
        except Exception as e:
            logging.error("get_resource Failed:{}".format(e))
            resp = None
        if resp is None:
            raise RuntimeError("Failed to fetch cloud resource of type: {}, id: {} and filter: {}".format(res_type, res_id, res_filter))
            
        return resp
    
    
    def stop(self):
        logger.info("stopping connector")
        self.start=False
        self.check_connection_thread.join()
        self.sender_connection.close()
        try:
            self.receiver_connection.close()
        except:
            pass
        self.consume_thread.join()
    
        
if __name__ == '__main__':
    
    sender_property={
        "exchange":"RESOURCES_UPDATES",
        "exchange_type":"topic",
        "passive":False,
        "durable":True,
        "auto_delete":False,
    }
    receiver_property={
        "exchange":"RESOURCES_UPDATES",
        "exchange_type":"topic",
        "passive":False,
        "durable":True,
        "auto_delete":False,
        "subscriptions":[{
                    "camera":'5f23a652c9ca28c957a4f39e',
                    'topic':'behaviours',
                    'eventType': 'Updated'
                },
                {
                    "organization":'5e78cc9e4cdd86de92d5db83',
                    'topic':'#',
                    'eventType': 'Updated'
                }],
        "queue":"standard",
        "consumerTopics":["mask-detection","occupancy"]
    }
    
    
    rabbit_server_config={
        'host':'queue.vedalabs.in',
        'user':'guest',
        'password':'guest',
        'port':5672
    }
    
    connector=RabbitMqConnector(host="queue.vedalabs.in",callback=callback,consumerSubscriptions=[{
                    "camera":'5f23a652c9ca28c957a4f39e',
                    'topic':'behaviours',
                    'eventType': 'Updated'
                },
                {
                    "organization":'5e78cc9e4cdd86de92d5db83',
                    'topic':'#',
                    'eventType': 'Updated'
                }],consumerTopics=["mask"],producerTopic="tortoise")

    
    
        
        