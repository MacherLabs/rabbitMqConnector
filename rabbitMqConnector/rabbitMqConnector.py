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

mapnonprint = {
	'\0':'^@',
	'\1':'^A',
	'\2':'^B',
	'\3':'^C',
	'\4':'^D',
	'\5':'^E',
	'\6':'^F',
	'\a':'^G',
	'\b':'^H',
	'\t':'^I',
	'\n':'^J',
	'\v':'^K',
	'\f':'^L',
	'\r':'^M',
	'\x00':'^@',
	'\x01':'^A',
	'\x02':'^B',
	'\x03':'^C',
	'\x04':'^D',
	'\x05':'^E',
	'\x06':'^F',
	'\x07':'^G',
	'\x08':'^H',
	'\x09':'^I',
	'\x0a':'^J',
	'\x0b':'^K',
	'\x0c':'^L',
	'\x0d':'^M',
	'\x0e':'^N',
	'\x0f':'^O',
	'\x10':'^P',
	'\x11':'^Q',
	'\x12':'^R',
	'\x13':'^S',
	'\x14':'^T',
	'\x15':'^U',
	'\x16':'^V',
	'\x17':'^W',
	'\x18':'^X',
	'\x19':'^Y',
	'\x1a':'^Z',
	'\x1b':'^[',
	'\x1c':'^\\',
	'\x1d':'^]',
	'\x1e':'^^',
	'\x1f':'^-',
}


def callback(message,props,method):
    print("="*50)
    print("Consuming Message")
    print("consumer topic",method.routing_key)
    print("="*50)
    logger.info("message received finally-{}".format(message))
    
    

        
class RabbitMqConnector():
    
    def __init__(self,rabbit_server_config,
                 topicCallback=callback,
                 subscriptionCallback=callback,
                 consumerSubscriptions=None,
                 consumerTopics=None,
                 consumerSyncTopics=None,
                 producerSubscriptions=None,
                 producerTopic=None,**kwargs):
       
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
        "consumerTopics":consumerTopics,
        "consumerSyncTopics":consumerSyncTopics,
        "consumerSyncField":kwargs.get("consumerSyncField",None),
        "queueId":kwargs.get('queueId',"")
        }
        rest_api_config=kwargs.get("rest_api_config",{})
        if (producerSubscriptions is not None or consumerSubscriptions is not None) and rest_api_config=={}:
            logger.error("rest_api_config is mandatory for susbscriptions")
            return 
       
        
        logger.info("rabbit server config-{}, rest_api_config-{} ,sender_properties-{} ,receiver_properties-{}".format(rabbit_server_config,rest_api_config,sender_properties,receiver_properties))
        self.rabbit_server_config=rabbit_server_config
        self.sender_rabbit_server_config=kwargs.get("sender_rabbit_server_config",rabbit_server_config)
        self.receiver_rabbit_server_config=kwargs.get("receiver_rabbit_server_config",rabbit_server_config)
        self.sender_creds=pika.PlainCredentials(self.sender_rabbit_server_config['user'], self.sender_rabbit_server_config['password'])
        self.receiver_creds=pika.PlainCredentials(self.receiver_rabbit_server_config['user'], self.receiver_rabbit_server_config['password'])
        self.heartbeat=30
        self.start = True 
        self.subRouteMap={}
        self.subRoutes=[]
        self.consume_failure=0
        self.sender_properties=sender_properties
        self.receiver_properties=receiver_properties
        self.rest_api_config=rest_api_config
        self.create_hierarchy()
        self.topicCallback=topicCallback
        self.subscriptionCallback=subscriptionCallback
        self.failures=0
        self.check_connection_thread = threading.Thread(target=self.check_connection_state)
        
        try:
            self.init_connections()
        except Exception as e:
            logger.error("some error occurred initializing connection retrying-{}".format(str(e)))
            self.check_connection_thread.start()
                    
        if not self.check_connection_thread.is_alive():
            self.check_connection_thread.start()
        
        time.sleep(1)
                
            
    def init_connections(self):
        self.init_sender_connection()
        self.init_receiver_connection()
        
           
    def init_sender_connection(self):
        self.sender_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.sender_rabbit_server_config['host'],credentials=self.sender_creds,heartbeat=self.heartbeat))
        self.sender_channel=self.sender_connection.channel()
        self.sender_channel.exchange_declare(
            exchange=self.sender_properties["exchange"],
            exchange_type=self.sender_properties["exchange_type"],
            passive=self.sender_properties["passive"],
            durable=self.sender_properties["durable"],
            auto_delete=self.sender_properties["auto_delete"])
        
        
    def get_queue(self,topic):
        queueId=str(self.receiver_properties.get("queueId",""))
        return queueId+'-'+topic
    
    
    def init_receiver_connection(self):
        #import pdb;pdb.set_trace()
        self.receiver_connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.receiver_rabbit_server_config['host'],credentials=self.receiver_creds,heartbeat=self.heartbeat))
        self.receiver_channel=self.receiver_connection.channel()
        self.sync_receiver_channels={}
        self.receiver_queue_async=None
        self.receiver_channel.exchange_declare(
            exchange=self.receiver_properties["exchange"],
            exchange_type=self.receiver_properties["exchange_type"],
            passive=self.receiver_properties["passive"],
            durable=self.receiver_properties["durable"],
            auto_delete=self.receiver_properties["auto_delete"])

        consumerTopics=self.receiver_properties.get("consumerTopics",None)
        subscriptions=self.receiver_properties.get("subscriptions",None)
        consumerSyncTopics=self.receiver_properties.get("consumerSyncTopics",None)
        
        self.add_subscriptions(subscriptions,consumerTopics,consumerSyncTopics)
        
                
    def add_subscriptions(self,subscriptions=None,consumerTopics=None,consumerSyncTopics=None):
        #import pdb;pdb.set_trace()
        routingKeys=[]
        if consumerTopics is not None:
            for topic in consumerTopics:
                routingKeys.append(topic)
                
        if  subscriptions is not None:     
            for subscription in subscriptions:
                routingKey=self.getRoutingKey(subscription)
                routingKeys.append(routingKey)
                logger.info("subscribing to routes-{}".format(routingKey))
        
        if (len(routingKeys)>0):
            if not self.receiver_queue_async:
                self.receiver_queue_async=self.get_queue("")+'-async'
                self.receiver_channel.queue_declare(queue=self.receiver_queue_async)
                logger.info("subscribing to routes-{}, queue-{}".format(routingKeys,self.receiver_queue_async))
                routingKeys.append('test')
                for routingKey in routingKeys:
                    self.receiver_channel.queue_bind(queue=self.receiver_queue_async,exchange=self.receiver_properties["exchange"], routing_key=routingKey)
                self.receiver_channel.basic_consume(queue=self.receiver_queue_async, on_message_callback=self.receive, auto_ack=True)
                self.consume_thread = threading.Thread(target=self.consume)
                if not self.consume_thread.is_alive():
                    self.consume_thread.start()
            else:
                self.receiver_properties["subscriptions"].extend(subscriptions)
                self.reinit_receiver()
                    
        # If consumers are sync, make a different queue for each consumer  
        if consumerSyncTopics is not None:
            for consumerTopic in consumerSyncTopics:
                channel=self.receiver_connection.channel()
                channel.exchange_declare(
                    exchange=self.receiver_properties["exchange"],
                    exchange_type=self.receiver_properties["exchange_type"],
                    passive=self.receiver_properties["passive"],
                    durable=self.receiver_properties["durable"],
                    auto_delete=self.receiver_properties["auto_delete"])
                queue_name= self.get_queue(consumerTopic)
                channel.queue_declare(queue=queue_name) 
                channel.queue_bind(queue=queue_name,exchange=self.receiver_properties["exchange"], routing_key=consumerTopic)
                logger.info("listening to sync queue-{}".format(queue_name))
                self.sync_receiver_channels[consumerTopic]=channel    
                
    def reinit_connections(self):
        self.reinit_sender()
        self.reinit_receiver()
      
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
            self.receiver_channel.stop_consuming()
        except:
            logger.info("some error in stopping consuming")
            pass
            
        try:
            self.consume_thread.join()
        except:
            logger.info("some error in closing consumer thread")
            pass
        
        try:
            self.receiver_connection.close()
            self.receiver_channel.close()
        except Exception as e:
            logger.info("failed to reinitialize connection")
            pass
        
        self.init_receiver_connection()
        
    def check_connection_state(self):
        while(self.start):
            state='OK'
            try:
                time.sleep(self.heartbeat)
            
                logger.info("checking connection state with server")
                
                self.send(producerTopic='test',message={"message":"ping---------------pong"})
                        
                if self.sender_connection.is_closed or self.sender_channel.is_closed:
                    logger.info("sender connection closed")
                    state='BROKEN'
                    try:
                        self.reinit_sender()
                    except Exception as e:
                        logger.info("failed to open sender connection..retrying")
                        
                self.receiver_connection.process_data_events()
                if self.receiver_connection.is_closed or self.receiver_channel.is_closed:
                    logger.info("receiver connection closed")
                    state='BROKEN'
                    try:
                        self.reinit_receiver()
                    except Exception as e:
                        logger.info("failed to open receiver connection..retrying")
            except Exception as e:
                state='BROKEN'
                logger.info("failed to open connections..retrying")
                try:
                    self.reinit_connections()
                except:
                    pass
            logger.info("connection state->{}".format(state))
                
        
    def send(self,message={"message":"ping------pong"},subscription=None,producerTopic=None,showMessage=False):
    
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
            return
        
        if routingKey!= 'test':
            print("="*50)
            print("Producing Message")
            print("Producer topic",routingKey)
            print("="*50)
            
            if showMessage:
                logger.info("successfully sent message on routing key-{}, message-{}".format(routingKey,message))
            else:
                logger.info("successfully sent message on routing key-{}".format(routingKey))
            
    def send_response(self,props,message,showMessage=False):
        try:
            self.sender_channel.basic_publish(exchange='', routing_key=props.reply_to,properties=pika.BasicProperties(correlation_id = props.correlation_id),body=json.dumps(message))
        except Exception as e:
            logger.info("failed to send message-{}".format(str(e)))
            return
            
        if showMessage:
            logger.info("successfully sent message on routing key-{}, message-{}".format(props.reply_to,message))
        else:
            logger.info("successfully sent message on routing key-{}".format(props.reply_to))

    def replacecontrolchar(self,text):
        for a,b in mapnonprint.items():
            if a in text:
                logger.warning("Json Decode replacecontrolchar:{} with {}".format(a,b))
                text = text.replace(a,b)
        return text       
        
    def receive(self,ch, method, properties, body):
        message=body.decode('utf8').replace("'", '"')
        try:
            message=json.loads(message1)
        except Exception as e:
            try:
                message=json.loads(self.replacecontrolchar(message))
            except Exception as e:
                logger.info("error loading message to json-{}".format(str(e)))
        routingKey=method.routing_key

        if routingKey != 'test':
            print("="*50)
            print("Consuming Message")
            print("Consumer topic",routingKey)
            print("="*50)
            if routingKey in self.subRoutes:
                self.subscriptionCallback(message,properties,method)
            else:
                self.topicCallback(message,properties,method)
                
               
    def consume(self):
        time.sleep(5)
        logger.info("receiver started consuming")
        # wait for sometime
        try:
            self.receiver_channel.start_consuming()
        except Exception as e:
            logger.info("some error occured while consuming-{}".format(str(e)))
            self.consume_failure +=1
            if self.consume_failure<5 and self.start==True:
                self.consume()
            if self.start == True:
                self.reinit_receiver()
                
    def consume_sync(self,topic):
        method_frame, header_frame, body = self.sync_receiver_channels[topic].basic_get(self.get_queue(topic),auto_ack=True)
        if method_frame:
            try:
                message=body.decode('utf8').replace("'", '"')
                message=json.loads(message1)
            except Exception as e:
                try:
                    message=json.loads(self.replacecontrolchar(message))
                except Exception as e:
                    logger.info("error loading message to json-{}".format(str(e)))
            print("="*50)
            print("Consuming Message")
            print("Consumer topic",topic)
            print("="*50)
            return message
        return None
    
    def consume_sync_all(self):
        startTime=time.time()
        finalMessage={}
        topics=self.receiver_properties['consumerSyncTopics'].copy()
        topicLength=len(topics)
        syncId=None
        syncField=self.receiver_properties['consumerSyncField']
        while(len(topics)>0):
            for topic in topics:
                out=self.consume_sync(topic)
                if out:
                    finalMessage[topic]=out
                    topics.remove(topic)
           
            if len(topics)==topicLength:
                return None
                
            if (time.time()-startTime)>10:
                logger.info("sync time exceeded, failed to sync")
                self.flush_sync_consumer_queues()
                return None
        if syncField:
            for key in finalMessage.keys():
                id=finalMessage[key][syncField]
                if syncId:
                    if id!=syncId:
                        logger.info("consumers not in sync..flushing queue")
                        self.flush_sync_consumer_queues()
                        return None   
                else:
                    syncId=id
                    
                    
        return finalMessage
    
    def flush_sync_consumer_queues(self):
        for topic in self.receiver_properties['consumerSyncTopics']:
            self.sync_receiver_channels[topic].queue_purge(self.get_queue(topic))
        
        
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
        self.subRoutes.append(finalRoute)
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
        logger.info("stopping connector & ignoring all errors..helpless-please don't judge!")
        
        self.start=False
        try:
            self.check_connection_thread.join()
        except:
            pass
        try:
            self.sender_connection.close()
        except:
            pass
        
        try:
            self.receiver_channel.stop_consuming()
        except:
            pass
        
        try:
            self.receiver_connection.close()
        except:
            pass
        
        try:
            self.consume_thread.join()
        except:
            pass
    
        
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
        
        
