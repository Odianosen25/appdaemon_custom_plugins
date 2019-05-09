import asyncio
import copy
import ssl
import json
import psutil
from datetime import datetime
import calendar
import appdaemon.utils as utils
from appdaemon.appdaemon import AppDaemon
from appdaemon.plugin_management import PluginBase

import traceback
import appdaemon.utils as utils

class HwstatsPlugin(PluginBase):
    def __init__(self, ad: AppDaemon, name, args):
        super().__init__(ad, name, args)

        self.AD = ad
        self.stopping = False
        self.config = args
        self.name = name
        self.initialized = False
        self.getHWStats = None
        self.state = {}

        if "namespace" in self.config:
            self.namespace = self.config["namespace"]
        else:
            self.namespace = "default"
            
        self.logger.info("Hardware Stats Plugin Initializing")
        
        self.hw_sensors = self.config.get('sensors', [])

        self.loop = self.AD.loop # get AD loop
        self.state_Lock = asyncio.Lock(loop = self.loop)
        self.booted = datetime.fromtimestamp(psutil.boot_time()).replace(microsecond=0) #get system up time

        self.hwstats_metadata = {
                                        "version": "1.0",
                                        "sensors": self.hw_sensors                                      
                                    }

    def stop(self):
        self.logger.debug("stop() called for %s", self.name)
        self.logger.info("Stopping Hardware Stats Plugin")
        self.stopping = True

    #
    # Get initial state
    #

    async def get_complete_state(self):
        self.logger.debug("*** Sending Complete State: %s ***", self.state)
        return copy.deepcopy(self.state)

    async def get_metadata(self):
        return self.hwstats_metadata

    #
    # Utility gets called every second (or longer if configured
    # Allows plugin to do any housekeeping required
    #

    def utility(self):
        #self.logger.info("*** Utility ***".format(self.state))
        return

    #
    # Handle state updates
    #

    async def get_updates(self):
        first_time = True

        while not self.stopping: 
            if (self.getHWStats == None or self.getHWStats.cancelled()) and first_time:
                self.getHWStats = asyncio.ensure_future(utils.run_in_executor(self, self.get_sensor_states, first_time), loop = self.loop)

            state = await self.get_complete_state()
            meta = await self.get_metadata()

            if self.getHWStats != None and self.getHWStats.done() and first_time: #meaning the plugin running first time
                await self.AD.plugins.notify_plugin_started(self.name, self.namespace, meta, state, first_time)
                first_time = False
                self.logger.info("Hardware Stats Plugin initialization complete")

            elif self.getHWStats != None and self.getHWStats.done():
                self.getHWStats = asyncio.ensure_future(utils.run_in_executor(self, self.get_sensor_states, first_time), loop = self.loop)

            await asyncio.sleep(1)

    #
    # Set State
    #

    def get_namespace(self):
        return self.namespace
    
    def get_sensor_states(self, first_time):
        nowTime = datetime.now().replace(microsecond=0)

        entity_id = "sensor.datetime"
        state = nowTime.replace(microsecond = 0, second = 0).strftime("%H:%M")
        nowD = nowTime.date().strftime("%d/%m/%Y")
        nowday = list(calendar.day_name)[nowTime.weekday()]
        kwargs = {"state" : state, "attributes" : {"date" : nowD, "day" : nowday, "friendly_name" : "Date Time"}}
        if not first_time:
            self.loop.create_task(self.state_update(entity_id, kwargs))

        # get CPU sensors
        if "cpu" in self.hw_sensors:
            entity_id = "sensor.cpu_freq"
            data = psutil.cpu_freq()
            kwargs = {"state" : data.current, "attributes" : {"min" : data.min, "max" : data.max, "friendly_name" : "CPU Frequency"}}
            if not first_time:
                self.loop.create_task(self.state_update(entity_id, kwargs))

            entity_id = "sensor.cpu_count"
            data = psutil.cpu_count()
            kwargs = {"state" : data, "attributes" : {"friendly_name" : "CPU Count"}}
            if not first_time:
                self.loop.create_task(self.state_update(entity_id, kwargs))

        if "memory" in self.hw_sensors:
            entity_id = "sensor.virtual_memory"
            data = psutil.virtual_memory()
            kwargs = {"state" : data.percent, "attributes" : {"used" : data.used, "free" : data.free, "total" : data.total, "available" : data.available, 
                "active" : data.active, "inactive" : data.inactive, "buffers" : data.buffers, "cached" : data.cached, "shared" : data.shared, "friendly_name" : "Virtual Memory"}}
            if not first_time:
                self.loop.create_task(self.state_update(entity_id, kwargs))

            entity_id = "sensor.swap_memory"
            data = psutil.swap_memory()
            kwargs = {"state" : data.percent, "attributes" : {"used" : data.used, "free" : data.free, "total" : data.total, "friendly_name" : "Swap Memory"}}
            if not first_time:
                self.loop.create_task(self.state_update(entity_id, kwargs))

        if "temperature" in self.hw_sensors:
            data = psutil.sensors_temperatures()
            for k, v in data.items():
                entity_id = "sensor.{}_temperature".format(k)
                sensorData = v[0]
                kwargs = {"state" : sensorData.current, "attributes" : {"hgih" : sensorData.high, "critical" : sensorData.critical, "friendly_name" : "{} Temperature".format(k.capitalize())}}
                if not first_time:
                    self.loop.create_task(self.state_update(entity_id, kwargs))

        if "uptime" in self.hw_sensors:
            entity_id = "sensor.hardware_uptime"
            uptime = nowTime - self.booted
            state = str(uptime)
            days = uptime.days
            seconds = uptime.seconds
            kwargs = {"state" : state, "attributes" : {"days" : days, "seconds" : seconds, "friendly_name" : "Hardware Up Time"}}
            if not first_time:
                self.loop.create_task(self.state_update(entity_id, kwargs))

    async def state_update(self, entity_id, kwargs):
        self.logger.debug("Updating State for Entity_ID %s, with %s", entity_id, kwargs)

        if entity_id in self.state:
            old_state = self.state[entity_id]

        else:
            # Its a new state entry
            self.state[entity_id] = dict()
            old_state = {}
            old_state['attributes'] = {}

        new_state = copy.deepcopy(old_state)

        if 'attributes' not in new_state: #just to ensure
            new_state['attributes'] = {}

        if 'state' in kwargs:
            new_state['state'] = kwargs['state']
            del kwargs['state']

        if 'attributes' in kwargs:
            new_state['attributes'].update(kwargs['attributes'])

        else:
            new_state['attributes'].update(kwargs)

        try:
            last_changed = utils.dt_to_str((await self.AD.sched.get_now()).replace(microsecond=0), self.AD.tz) #possible AD isn't ready at this point
        except:
            last_changed = None
        
        new_state['last_changed'] = last_changed
        data = {'event_type': 'state_changed', 'data': {'entity_id': entity_id, 'new_state': new_state, 'old_state': old_state}}

        await self.AD.events.process_event(self.namespace, data)
        
        self.state[entity_id].update(new_state)
        return
