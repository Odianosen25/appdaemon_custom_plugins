# InfluxDB Appdaemon Plugin

This plugin enables the ability to have AD load up state data from entities, into an InfluxDB which is a TimeSeries Database
An example config can be seen below

```yaml

INFLUXDB:
    type: influxdb
    namespace: influxdb
    connection_url: http://xxx.xxx.xxx.xxx:8086
    token: !secret influxdb_token
    org: my_sensors
    timeout: 5000
    bucket: sensors
    databases:
        mqtt_sensors:
            bucket: mqtt_sensors
            tags:
                - siteId
            exclude_entities:
                - "*_motion_sensor"

        influxdb:
            tags:
                - siteId
            include_entities:
                - temperature.*
                - humidity.*
```

The setup follows the standard AD plugin setup which can be found [here](https://appdaemon.readthedocs.io/en/latest/CONFIGURE.html#plugins)
To configure the plugin, the following paramters are required:

- ``type:`` This must be declared and it must be ``influxdb``
- ``nammespace:`` (optional, str) This will default to ``default``, though optional better to use the namespace ``influxdb`` or some other name
- ``connection_url:`` (optional, str) The URL of the Influx Database. This will default to ``http://127.0.0.1:8086``
- ``token:`` This must be declared, and is the token to be used to accesss the database. More can be read [here](https://docs.influxdata.com/influxdb/v2.0/security/tokens/)
- ``org:`` This must be declared, and its the ``organization`` in the database the plugin is to store data in the database. More can be read [here](https://docs.influxdata.com/influxdb/v2.0/organizations/)
- ``timeout:`` (optional, int) The connection timeout to used, when accessing the database in milliseconds. This defaults to ``10000``
- ``bucket:`` This must be declared, and its the bucket where the data will be stored within the database. More can be read [here](https://docs.influxdata.com/influxdb/v2.0/organizations/buckets/). For flexibility, this can be either declared either top level, whereby all data to be stored using this plugin will enter a single bucket, at the ``databases`` level,
where each database has its own bucket.
- ``databases:`` This must be declared, and its essenatially the ``namespaces`` the plugin is to get data from. Each `database` as said earlier is a valid namespace within AD, and can be configured using the following
    - ``bucket:`` (optional, str) If wanting the data from the namespace to be in a certain bucket, this config here over rides the top level one if available
    - ``tags:`` (optional, list) When storing data into influxdb, it allows for each data to be tagged. This can be very userful for filtering and grouping data together. The plugin by default
    will add the `entity_id` as a tag to the stored data, and if this list is given, seek out the data as an attribute within the entity's data and also use it as a tag.
    - ``include_entities:`` (optional, list) If wanting to only store certain entities into the database. This supports the use of wildcards
    - ``exclude_entities:`` (optional, list) If wanting to exclude certain entities from being stored into the database. This supports the use of wildcards


Using the Plugin
=================

When working on your design to use this plugin, some certain things have to be put into considering. The plugin stores data as explained below.
Influxdb demands (outside time) the ``measurement`` and ``field`` values to be provided when storing data. This plugin uses the ``friendly_name``
of the ``entity_id`` as the measurement value, and the ``domain`` of the entity as the ``field``. So for example an entity_id ``temperature.living_room``, 
with a friendly_name "Living Room Temperature", and assuming it has an attribute of `siteId` within it with the value "living_room" and the plugin is setup to use `siteId` as a tag. 
The plugin will be broke down entity as having ``measurement`` as "Living Room Temperature", ``field`` as "temperature", and tags ``entity_id`` as "temperature.living_room" and 
``siteId`` as "living_room".

It is understood when working with different platforms like HASS, it doesn't give entities using such domains. So it will be up to the user to develop an app, which can convert the data as required into one the plugin understands, and store the entities within the plugin's namespace; since it is temporary. Then the plugin can be setup to pick the data from its own namespace.
This can be easily done with a few lines of code. But if wanting to convert data from say MQTT, it becomes easy as the user will simply have to do the following
- First decide what data you want to store, and how your entity_id will be formed based on the information above
- Also decide what friendly_names you want to use for each entity_id, and tags if needed
- Create an app within the MQTT namespace
- Listen to the topics using wildcards for what you need
- As the data comes in, create the entities within the database's namespace using the right domain
- Since you working with wildcards, it will work even if your sensors expand in the future.

An example app for the above can be seen below

```python

import adbase as ad

class MQTT2InfluxDB(ad.ADBase):

    def initialize(self):
        self.adbase = self.get_ad_api()
        self.mqtt = self.get_plugin_api("MQTT")
        self.influxdb = self.get_plugin_api("INFLUXDB")

        wildcard = "my_super_topic/#"

        if topic not in self.mqtt.get_plugin_config()["topics"]: #first check if it has been subscribed to, and if not subscribe
            self.mqtt.mqtt_subscribe(topic)

        self.mqtt.listen_event(self.mqtt_messages, "MQTT_MESSAGE", wildcard=wildcard)
        

    def mqtt_messages(self, event, data, kwargs):
        self.adbase.log(f"__function__: {event}, {data}, {kwargs}", level="DEBUG")

        topic = data["topic"]
        state = int(data["payload])

        # we assume a topic like "temperature/Living Room"
        location = topic.split("/")[1]
        friendly_name = f"{location} Temperature Sensor"
        siteId = location.lower().replace(" ", "_")
        # lets form the entiy_id into temperature.living_room
        domain = topic.split("/")[0]
        entity_id = f"{domain}.{siteId}"
        attributes = {"friendly_name": friendly_name, "siteId": siteId}
        
        if not self.influxdb.entity_exists(entity_id):
            # it doesn't exist yet
            self.influxdb.add_entity(entity_id, "None", attributes)
        
        # now we update our entity with the data
        self.influxdb.set_state(entity_id, state=state, attributes=attributes)
```

By using the example app, and as long as the plugin is setup to include entities ``temperature.*``, the right entities will always be picked up
even if the system was to expand, with no extra input from the user.