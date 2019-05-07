import copy
import AWSIoTPythonSDK.MQTTLib as AWSIoTPyMQTT
from AWSIoTPythonSDK.exception import AWSIoTExceptions
import asyncio
import traceback

import appdaemon.utils as utils
from appdaemon.appdaemon import AppDaemon
from appdaemon.plugin_management import PluginBase

class AwsiotPlugin(PluginBase):

    def __init__(self, ad: AppDaemon, name, args):
        super().__init__(ad, name, args)

        """Initialize AWSIoT Plugin."""
        self.AD = ad
        self.stopping = False
        self.config = args
        self.name = name
        self.awsiot_connected = False
        self.state = {}

        self.logger.info("AWSIoT Plugin Initializing")

        self.name = name

        if 'namespace' in self.config:
            self.namespace = self.config['namespace']
        else:
            self.namespace = 'default'

        if 'verbose' in self.config:
            self.verbose = self.config['verbose']
        else:
            self.verbose = False

        self.awsiot_endpoint = self.config.get('client_endpoint')
        self.awsiot_port = self.config.get('client_port', 8883)

        if self.awsiot_endpoint == None:
            self.logger.critical("Please connect to a valid AWS EndPoint for the AWSIoT Service")
            return

        self.awsiot_qos = self.config.get('client_qos', 0)
        awsiot_id = self.config.get('client_Id', None)
        awsiot_useWebsockets = self.config.get('useWebsockets', False)
        awsiot_session = self.config.get('client_clean_session', True)
        self.awsiot_topics = self.config.get('client_topics', ['#'])
        self.awsiot_user = self.config.get('client_user', None)
        self.awsiot_password = self.config.get('client_password', None)
        self.awsiot_event_name = self.config.get('event_name', 'AWSIOT_MESSAGE')
        self.awsiot_baseReconnectQuietTimeSecond = self.config.get('baseReconnectQuietTimeSecond', 1)
        self.awsiot_maxReconnectQuietTimeSecond = self.config.get('maxReconnectQuietTimeSecond', 128)
        self.awsiot_stableConnectionTimeSecond = self.config.get('stableConnectionTimeSecond', 20)
        self.awsiot_enableMetrics = self.config.get('enableMetrics', True)
        self.awsiot_timeout = self.config.get('client_timeout', 120)
        self.offlinePublishing = self.config.get('offlinePublishing', -1)
        self.operationalTimeout = self.config.get('operationalTimeout', 5)
        self.drainingFrequency = self.config.get('drainingFrequency', 2)

        status_topic = '{}_status'.format(self.config.get('client_id', self.name + ' client').lower())
        status_topic = status_topic.replace('_', '/')
        
        self.awsiot_will_topic = self.config.get('will_topic', None)
        self.awsiot_onOnline_topic = self.config.get('birth_topic', None)
        self.awsiot_will_retain = self.config.get('will_retain', True)
        self.awsiot_onOnline_retain = self.config.get('birth_retain', True)

        if self.awsiot_will_topic == None:
            self.awsiot_will_topic = status_topic
            self.logger.info("Using %r as Will Topic", status_topic)
        
        if self.awsiot_onOnline_topic == None:
            self.awsiot_onOnline_topic = status_topic
            self.logger.info("Using %r as Birth Topic", status_topic)

        self.awsiot_will_payload = self.config.get('will_payload', 'offline')
        self.awsiot_onOnline_payload = self.config.get('birth_payload', 'online')
        self.awsiot_shutdown_payload = self.config.get('shutdown_payload', self.awsiot_will_payload)

        self.awsiot_ca_cert = self.config.get('ca_cert')
        self.awsiot_client_cert = self.config.get('client_cert')
        self.awsiot_client_key = self.config.get('client_key')

        if awsiot_id == None:
            awsiot_id = 'ad_aws_{}_client'.format(self.name.lower())
            self.logger.info("Using %s as Client ID", awsiot_id)

        self.awsiot = AWSIoTPyMQTT.AWSIoTMQTTClient(awsiot_id, useWebsocket=awsiot_useWebsockets)

        self.awsiot.onOnline = self.awsiot_onOnline
        self.awsiot.onOffline = self.awsiot_onOffline
        #self.awsiot.on_message = self.awsiot_on_message

        self.loop = self.AD.loop # get AD loop
        self.awsiot_connect_event = asyncio.Event(loop = self.loop)
        self.awsiot_wildcards = list()
        self.awsiot_metadata = {
            "version": "1.0",
            "endpoint" : self.awsiot_endpoint,
            "port" : self.awsiot_port,
            "client_id" : awsiot_id,
            "useWebsockets" : awsiot_useWebsockets,
            "clean_session": awsiot_session,
            "qos" : self.awsiot_qos,
            "topics" : self.awsiot_topics,
            "username" : self.awsiot_user,
            "password" : self.awsiot_password,
            "event_name" : self.awsiot_event_name,
            "status_topic" : status_topic,
            "will_topic" : self.awsiot_will_topic,
            "will_payload" : self.awsiot_will_payload,
            "will_retain" : self.awsiot_will_retain,
            "birth_topic" : self.awsiot_onOnline_topic,
            "birth_payload" : self.awsiot_onOnline_payload,
            "birth_retain" : self.awsiot_onOnline_retain,
            "shutdown_payload" : self.awsiot_shutdown_payload,
            "ca_cert" : self.awsiot_ca_cert,
            "client_cert" : self.awsiot_client_cert,
            "client_key" : self.awsiot_client_key,
            "timeout" : self.awsiot_timeout,
            "operationalTimeout" : self.operationalTimeout,
            "offlinePublishing" : self.offlinePublishing
                            }

    def stop(self):
        self.logger.debug("stop() called for %s", self.name)
        self.stopping = True
        if self.awsiot_connected:
            self.logger.info("Stopping AWSIoT Plugin and Unsubcribing from URL %s:%s", self.awsiot_endpoint, self.awsiot_port)
            for topic in self.awsiot_topics:
                self.logger.debug("Unsubscribing from Topic: %s", topic)
                try:
                    result = self.awsiot.unsubscribe(topic)
                    if result:
                        self.logger.debug("Unsubscription from Topic %r Successful", topic)
                except AWSIoTExceptions.unsubscribeTimeoutException as au:
                    self.logger.critical("There was an Unsubscription error %r, while trying to Unsubscribe from Topic %s", au, topic)
                    self.logger.debug("There was an Unsubscription error %r, while trying to Unsubscribe from Topic %s with Traceback: %s", au, topic, traceback.format_exc())
        
            try:                    
                self.awsiot.publish(self.awsiot_will_topic, self.awsiot_shutdown_payload, self.awsiot_qos)
                self.awsiot.disconnect() #disconnect cleanly
            except AWSIoTExceptions.disconnectError as ad:
                self.logger.critical("There was a Disconnection error %r, while trying to stop the AWSIoT Service", ad)
                self.logger.debug("There was a Disconnection error %r, while trying to stop the AWSIoT Service with Traceback: %s", ad, traceback.format_exc())
            except AWSIoTExceptions.disconnectTimeoutException:
                self.logger.critical("There was a Time Out Disconnection error while trying to stop the AWSIoT Service")
                self.logger.debug("There was a Time Out Disconnection error while trying to stop the AWSIoT Service with Traceback: %s", traceback.format_exc())

    def awsiot_onOnline(self):
        self.awsiot_connected = True

        self.awsiot.publish(self.awsiot_onOnline_topic, self.awsiot_onOnline_payload, self.awsiot_qos)

        self.logger.info("Connected to Endpoint at URL %s:%s", self.awsiot_endpoint, self.awsiot_port)
        #
        # Register AWSIoT Services
        #
        self.AD.services.register_service(self.namespace, "awsiot", "subscribe", self.call_plugin_service)
        self.AD.services.register_service(self.namespace, "awsiot", "unsubscribe", self.call_plugin_service)
        self.AD.services.register_service(self.namespace, "awsiot", "publish", self.call_plugin_service)

        data = {'event_type': self.awsiot_event_name, 'data': {'state': 'Connected', 'topic' : None, 'wildcard' : None}}
        self.loop.create_task(self.send_ad_event(data))

        self.awsiot_connect_event.set() # continue processing

    def awsiot_onOffline(self):
        if not self.stopping: #unexpected disconnection
            self.awsiot_connected = False
            self.logger.critical("AWSIoT Client Disconnected Abruptly. Will attempt reconnection")

            data = {'event_type': self.awsiot_event_name, 'data': {'state': 'Disconnected', 'topic' : None, 'wildcard' : None}}
            self.loop.create_task(self.send_ad_event(data))

    def awsiot_on_message(self, client, userdata, msg):
        try:
            self.logger.debug("Message Received: Topic = %s, Payload = %s", msg.topic, msg.payload)
            topic = msg.topic

            if self.awsiot_wildcards != [] and list(filter(lambda x: x in topic, self.awsiot_wildcards)) != []: #check if any of the wildcards belong
                wildcard = list(filter(lambda x: x in topic, self.awsiot_wildcards))[0] + '#'

                data = {'event_type': self.awsiot_event_name, 'data': {'topic': topic, 'payload': msg.payload.decode(), 'wildcard': wildcard}}

            else:
                data = {'event_type': self.awsiot_event_name, 'data': {'topic': topic, 'payload': msg.payload.decode(), 'wildcard': None}}

            self.loop.create_task(self.send_ad_event(data))
        except:
            self.logger.critical("There was an error while processing an AWSIoT message")
            self.logger.debug('There was an error while processing an AWSIoT message, with Traceback: %s', traceback.format_exc())


    async def call_plugin_service(self, namespace, domain, service, kwargs):
        if 'topic' in kwargs:
            if not self.awsiot_connected:  # ensure AWSIoT plugin is connected
                self.logger.debug("Attempt to call AWSIoT Service while disconnected: %s", service)
            try:
                topic = kwargs['topic']
                payload = kwargs.get('payload', None)
                retain = kwargs.get('retain', False)
                qos = int(kwargs.get('qos', self.awsiot_qos))

                if service == 'publish':
                    self.logger.debug("Publish Payload: %s to Topic: %s", payload, topic)

                    result = await utils.run_in_executor(self, self.awsiot.publish, topic, payload, qos)

                    if result:
                        self.logger.debug("Publishing Payload %s to Topic %s Successful", payload, topic)

                elif service == 'subscribe':
                    self.logger.debug("Subscribe to Topic: %s", topic)

                    if topic not in self.awsiot_topics:
                        result = await utils.run_in_executor(self, self.topic_subscribe, topic, qos)

                        if result:
                            self.logger.debug("Subscription to Topic %s Sucessful", topic)
                            self.awsiot_topics.append(topic)
                        else:
                            self.logger.warning("Subscription to Topic %s was not Sucessful", topic)
                    else:
                        self.logger.info("Topic %s already subscribed to", topic)

                elif service == 'unsubscribe':
                    self.logger.debug("Unsubscribe from Topic: %s", topic)

                    result = await utils.run_in_executor(self, self.awsiot.unsubscribe, topic)
                    if result:
                        self.logger.debug("Unsubscription from Topic %s Successful", topic)
                        if topic in self.awsiot_topics:
                            self.awsiot_topics.remove(topic)

                else:
                    self.logger.warning("Wrong Service Call %s for AWSIoT", service)
                    result = 'ERR'

            except Exception as e:
                config = self.config
                if config['type'] == 'awsiot':
                    self.logger.debug('Got the following Error %s, when trying to retrieve AWSIoT Plugin', e)
                    return str(e)
                else:
                    self.logger.critical(
                        'Wrong Namespace %s selected for AWSIoT Service. Please use proper namespace before trying again',
                        namespace)
                    return 'ERR'
        else:
            self.logger.warning('Topic not provided for Service Call {!r}.'.format(service))
            raise ValueError("Topic not provided, please provide Topic for Service Call")

        return result

    async def process_awsiot_wildcard(self, wildcard):
        if wildcard.rstrip('#') not in self.awsiot_wildcards:
            self.awsiot_wildcards.append(wildcard.rstrip('#'))

    async def awsiot_state(self):
        return self.awsiot_connected
    
    async def send_ad_event(self, data):
        await self.AD.events.process_event(self.namespace, data)

    #
    # Get initial state
    #

    async def get_complete_state(self):
        self.logger.debug("*** Sending Complete State: %s ***", self.state)
        return copy.deepcopy(self.state)

    async def get_metadata(self):
        return self.awsiot_metadata

    #
    # Utility gets called every second (or longer if configured
    # Allows plugin to do any housekeeping required
    #

    def utility(self):
        #self.logger.info("utility".format(self.state)
        return

    #
    # Handle state updates
    #

    async def get_updates(self):
        already_initialized = False
        already_notified = False
        first_time = True

        try:
            await utils.run_in_executor(self, self.start_awsiot_service)
            await asyncio.wait_for(self.awsiot_connect_event.wait(), 5, loop=self.loop) # wait for it to return true for 5 seconds in case still processing connect
        except asyncio.TimeoutError:
            self.logger.critical("Could not Complete Connection to Endpoint, please Ensure Endpoint at URL %s:%s is correct and Endpoint is not down and restart Appdaemon", self.awsiot_endpoint, self.awsiot_port)

        while (self.awsiot_connected or not already_initialized) and not self.stopping: #continue as long as tried to connect
            state = await self.get_complete_state()
            meta = await self.get_metadata()

            if self.awsiot_connected : #meaning the client has connected to the Endpoint
                await self.AD.plugins.notify_plugin_started(self.name, self.namespace, meta, state, first_time)
                already_notified = False
                already_initialized = True
                self.logger.info("AWSIoT Plugin initialization complete")
            else:
                if not already_notified and already_initialized:
                    await self.AD.plugins.notify_plugin_stopped(self.name, self.namespace)
                    self.logger.critical("AWSIoT Plugin Stopped Unexpectedly")
                    already_notified = True
                    already_initialized = False
                    first_time = False
                if not already_initialized and not already_notified:
                    self.logger.critical("Could not complete AWSIoT Plugin initialization")
                else:
                    self.logger.critical("Unable to reinitialize AWSIoT Plugin, will keep trying again until complete")

            if not self.stopping:
                break

            asyncio.sleep(5)

    def get_namespace(self):
        return self.namespace

    def start_awsiot_service(self):
        try:
            self.awsiot_connect_event.clear() # used to wait for connection
            self.awsiot.configureEndpoint(self.awsiot_endpoint, self.awsiot_port)
            self.awsiot.configureLastWill(self.awsiot_will_topic, self.awsiot_will_payload, self.awsiot_qos)
            self.awsiot.configureCredentials(self.awsiot_ca_cert,self.awsiot_client_key, self.awsiot_client_cert)
            self.awsiot.configureOfflinePublishQueueing(self.offlinePublishing)
            self.awsiot.configureMQTTOperationTimeout(self.operationalTimeout)
            self.awsiot.configureDrainingFrequency(self.drainingFrequency)
            self.awsiot.configureAutoReconnectBackoffTime(self.awsiot_baseReconnectQuietTimeSecond, self.awsiot_maxReconnectQuietTimeSecond, self.awsiot_stableConnectionTimeSecond)
            if self.awsiot_enableMetrics:
                self.awsiot.enableMetricsCollection()
            else:
                self.awsiot.disableMetricsCollection()

            res = self.awsiot.connect(keepAliveIntervalSecond=self.awsiot_timeout)

            if res:
                for topic in self.awsiot_topics:
                    self.logger.debug("Subscribing to Topic: %s", topic)
                    result = self.topic_subscribe(topic, self.awsiot_qos)
                    if result:
                        self.logger.debug("Subscription to Topic %s Sucessful", topic)
                    else:
                        self.awsiot_topics.remove(topic)
                        self.logger.debug("Subscription to Topic %r Unsucessful, as Client possibly not currently connected", topic)

        except AWSIoTExceptions.connectTimeoutException as ae:
            self.logger.critical("There was a Time Out Connection error while trying to setup the AWSIoT Service, as %s", ae)
            self.logger.debug("There was a Time Out Connection error while trying to setup the AWSIoT Service with Traceback: %s", traceback.format_exc())
        return

    def topic_subscribe(self, topic, qos):
        res = False
        try:
            res = self.awsiot.subscribe(topic, qos, self.awsiot_on_message)
        except AWSIoTExceptions.subscribeTimeoutException as se:
            self.logger.critical("There was a Subscription error %r, while trying to Subscribe to Topic %r", se, topic)
            self.logger.debug("There was a Subscription error %r, while trying to Subscribe to Topic %r with Traceback: %s", se, topic, traceback.format_exc())

        return res

