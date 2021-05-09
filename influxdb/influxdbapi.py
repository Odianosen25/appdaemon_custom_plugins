import appdaemon.adbase as adbase
import appdaemon.adapi as adapi
from appdaemon.appdaemon import AppDaemon
import appdaemon.utils as utils


class Influxdb(adbase.ADBase, adapi.ADAPI):

    # entities = Entities()

    def __init__(
        self, ad: AppDaemon, name, logging, args, config, app_config, global_vars,
    ):

        # Call Super Classes
        adbase.ADBase.__init__(self, ad, name, logging, args, config, app_config, global_vars)
        adapi.ADAPI.__init__(self, ad, name, logging, args, config, app_config, global_vars)

    #
    # Helper Functions
    #

    @utils.sync_wrapper
    async def get_history(self, **kwargs):
        """Gets access to the AD's Database.
        This is a convenience function that allows accessing the AD's Database, so the
        history state of a device can be retrieved. It allows for a level of flexibility
        when retrieving the data, and returns it as a dictionary list. Caution must be
        taken when using this, as depending on the size of the database, it can take
        a long time to process. This function only works when using any of appdaemon's database
        Args:
            **kwargs (optional): Zero or more keyword arguments.
        Keyword Args:
            entity_id (str, optional): Fully qualified id of the device to be querying, e.g.,
                ``mqtt.office_lamp`` or ``sequence.ligths_on`` This can be any entity_id
                in the database. If this is left empty, the state of all entities will be
                retrieved within the specified time.
            bucket (str, optional): The bucket the expected data is requested from. This must be one of
                the pre-defined buckets setup in influxdb. If not specifed, it uses the default on set in the plugin
            measurement (str, optional): The measurement of the data to be read from the database. This is usually
                the friendly_name of the entity_id of the stored data.This will be used to filter
                the results that will be returned back from the operation
            field (str, optional): The field of the data to be read from the database. This will be used to filter
                the results that will be returned back from the operation
            filter_tags (dict, optional): The tags to be read from the database, with their respective values.
                This will be used to filter the results that will be returned back from the operation
            days (int, optional): The days from the present-day walking backwards that is
                required from the database. Thos defaults to 1
            start_time (str | datetime, optional): The start time from when the data should be retrieved.
                This should be the furthest time backwards, like if we wanted to get data from
                now until two days ago. Your start time will be the last two days datetime.
                ``start_time`` time can be either a UTC aware time string like ``2019-04-16 12:00:03+01:00``
                or a ``datetime.datetime`` object.
            end_time (str | datetime, optional): The end time from when the data should be retrieved. This should
                be the latest time like if we wanted to get data from now until two days ago. Your
                end time will be today's datetime ``end_time`` time can be either a UTC aware time
                string like ``2019-04-16 12:00:03+01:00`` or a ``datetime.datetime`` object. It should
                be noted that it is not possible to declare only ``end_time``. If only ``end_time``
                is declared without ``start_time`` or ``days``, it will revert to default to the latest
                history state.
            callback (callable, optional): If wanting to access the database to get a large amount of data,
                using a direct call to this function will take a long time to run and lead to AD cancelling the task.
                To get around this, it is better to pass a function, which will be responsible of receiving the result
                from the database. The signature of this function follows that of a scheduler call.
            namespace (str, optional): Namespace to use for the call, which the database is functioning. See the section on
                `namespaces <APPGUIDE.html#namespaces>`__ for a detailed description.
                In most cases it is safe to ignore this parameter.
        Returns:
            An iterable list of entity_ids/events and their history.

        Examples:
            Get device state over the last 5 days.
            >>> data = self.get_history(entity_id="light.office_lamp", days=5)
            Get all data from yesterday and walk 5 days back from the bucket sensors.
            >>> import datetime
            >>> from datetime import timedelta
            >>> end_time = datetime.datetime.now() - timedelta(days = 1)
            >>> data = self.get_history(end_time=end_time, days=5, bucket="sensors")
        """

        namespace = self._get_namespace(**kwargs)
        plugin = await self.AD.plugins.get_plugin_object(namespace)

        if hasattr(plugin, "get_history"):
            callback = kwargs.pop("callback", None)
            if callback is not None and callable(callback):
                self.create_task(plugin.get_history(**kwargs), callback)

            else:
                return await plugin.get_history(**kwargs)

        else:
            self.logger.warning(
                "Wrong Namespace selected, as %s has no database plugin attached to it", namespace,
            )
            return None

    @utils.sync_wrapper
    async def get_write_api(self, **kwargs):
        """Gets access to the Database's write api object.
        This can be useful if wanting to run some custom code within an app,
        which is not readily avialable in the plugin.
        Args:
            **kwargs (optional): Zero or more keyword arguments.
        Keyword Args:
        namespace (str, optional): Namespace to use for the call, which the database is functioning. See the section on
                `namespaces <APPGUIDE.html#namespaces>`__ for a detailed description.
                In most cases it is safe to ignore this parameter.
        Returns:
            The Influxdb writer object.

        Examples:
            Get get the writer object for the database
            >>> self.write_api = self.get_write_api()
            >>> # now writer custom data to the database
            >>> self.write_api.write("my-bucket", "my-org", {"measurement": "h2o_feet",
                "tags": {"location": "coyote_creek"}, "fields": {"water_level": 1.0}, "time": 1})
        """

        namespace = self._get_namespace(**kwargs)
        plugin = await self.AD.plugins.get_plugin_object(namespace)

        if hasattr(plugin, "get_write_api"):
            return plugin.get_write_api

        return None

    @utils.sync_wrapper
    async def get_query_api(self, **kwargs):
        """Gets access to the Database's query api object.
        This can be useful if wanting to run some custom code within an app,
        which is not readily avialable in the plugin.
        Args:
            **kwargs (optional): Zero or more keyword arguments.
        Keyword Args:
        namespace (str, optional): Namespace to use for the call, which the database is functioning. See the section on
                `namespaces <APPGUIDE.html#namespaces>`__ for a detailed description.
                In most cases it is safe to ignore this parameter.
        Returns:
            The Influxdb query object.

        Examples:
            Get get the query object for the database
            >>> self.query_api = self.get_query_api()
            >>> # now read custom data from the database
            >>> records = self.query_api.query_stream('from(bucket:"my-bucket") |> range(start: -10m)')
            >>> # now interate over records
            >>> for record in records:
            >>>     self.log(f'Temperature in {record["location"]} is {record["_value"]}')
        """
        namespace = self._get_namespace(**kwargs)
        plugin = await self.AD.plugins.get_plugin_object(namespace)

        if hasattr(plugin, "get_query_api"):
            return plugin.get_query_api

        return None
