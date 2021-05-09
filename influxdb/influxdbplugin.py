import asyncio
import copy
from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime, timedelta
import iso8601

from appdaemon.appdaemon import AppDaemon
from appdaemon.plugin_management import PluginBase
import appdaemon.utils as utils

import traceback


CONST_TRUE_STATES = ("on", "y", "yes", "true", "home", "opened", "unlocked", True)
CONST_FALSE_STATES = ("off", "n", "no", "false", "away", "closed", "locked", False)


class InfluxdbPlugin(PluginBase):
    def __init__(self, ad: AppDaemon, name, args):
        super().__init__(ad, name, args)

        self.AD = ad
        self.stopping = False
        self.config = args
        self.name = name
        self.initialized = False
        self.state = {}
        self._namespaces = {}
        self._client = None
        self._write_api = None
        self._query_api = None

        if "namespace" in self.config:
            self.namespace = self.config["namespace"]
        else:
            self.namespace = "default"

        self.logger.info("Influx Database Plugin Initializing")
        self._connection_url = self.config.get("connection_url", "http://127.0.0.1:8086")
        self._databases = self.config.get("databases", {})
        self._bucket = self.config.get("bucket")

        self._org = self.config.get("org")
        self._token = self.config.get("token")

        if not all([self._org, self._token]):
            raise ValueError("Cannot setup the Plugin, as all 'org' and 'token' settings must be given")

        if not isinstance(self._databases, dict):
            raise ValueError("The database setting is not Valid")

        self._timeout = self.config.get("timeout")
        self._connection_pool_maxsize = int(self.config.get("connection_pool_maxsize", 100))
        self._verify_ssl = self.config.get("verify_ssl", False)
        self._ssl_ca_cert = self.config.get("ssl_ca_cert")

        if self._connection_pool_maxsize < 5:
            self.logger.warning(
                "Cannot use %s for Connection Pool, must be higher than 5. Reverting to 100",
                self._connection_pool_maxsize,
            )
            self._connection_pool_maxsize = 100

        self.loop = self.AD.loop  # get AD loop

        self.database_metadata = {
            "version": "1.0",
            "connection_url": self._connection_url,
            "bucket": self._bucket,
            "org": self._org,
            "timeout": self._timeout,
            "verify_ssl": self._verify_ssl,
            "ssl_ca_cert": self._ssl_ca_cert,
        }

    def stop(self):
        self.logger.debug("stop() called for %s", self.name)

        self.stopping = True
        # set to continue
        self._event.set()

        self.logger.info("Stopping Influx Database Plugin")

        if self._client:
            self._client.close()

    #
    # Placeholder for constraints
    #
    def list_constraints(self):
        return []

    #
    # Get initial state
    #

    async def get_complete_state(self):
        self.logger.debug("*** Sending Complete State: %s ***", self.state)
        return copy.deepcopy(self.state)

    async def get_metadata(self):
        return self.database_metadata

    #
    # Utility gets called every second (or longer if configured
    # Allows plugin to do any housekeeping required
    #

    def utility(self):
        # self.logger.info("*** Utility ***".format(self.state))
        return

    #
    # Handle state updates
    #

    async def get_updates(self):
        already_notified = False
        first_time = True
        self.reading = False
        self._event = asyncio.Event()

        # set to continue
        self._event.set()

        while not self.stopping:
            await self._event.wait()

            if self.stopping is True:
                return

            try:
                if self._client is None:  # it has not been set
                    client_options = {"connection_pool_maxsize": self._connection_pool_maxsize}

                    if self._timeout is not None:
                        client_options["timeout"] = self._timeout

                    if self._verify_ssl is True:
                        client_options["verify_ssl"] = True

                    if self._ssl_ca_cert is not None:
                        client_options["ssl_ca_cert"] = self._ssl_ca_cert

                    self._client = await utils.run_in_executor(
                        self,
                        InfluxDBClient,
                        url=self._connection_url,
                        token=self._token,
                        org=self._org,
                        **client_options,
                    )
                    self._write_api = self._client.write_api(write_options=SYNCHRONOUS)
                    self._query_api = self._client.query_api()

                if self._client is not None:
                    self.logger.info("Connected to Database using URL %s", self._connection_url)
                    states = await self.get_complete_state()

                    self.AD.services.register_service(
                        self.namespace, "influx", "write", self.call_plugin_service,
                    )
                    self.AD.services.register_service(
                        self.namespace, "influx", "read", self.call_plugin_service,
                    )
                    self.AD.services.register_service(
                        self.namespace, "influx", "get_history", self.call_plugin_service,
                    )

                    # now we register for the different namespaces
                    for ns, settings in self._databases.items():
                        if ns not in self._namespaces:
                            self._namespaces[ns] = {}

                            # we check for tags
                            ns_tags = settings.get("tags", [])
                            if isinstance(ns_tags, str):
                                ns_tags = [ns_tags]

                            self._namespaces[ns]["tags"] = ns_tags
                            self._namespaces[ns]["bucket"] = settings.get("bucket", self._bucket)

                        self._namespaces[ns]["handle"] = await self.AD.events.add_event_callback(
                            self.name, ns, self.event_callback, "state_changed", __silent=True, __namespace=ns,
                        )

                    await self.AD.plugins.notify_plugin_started(
                        self.name, self.namespace, self.database_metadata, states, first_time,
                    )

                    first_time = False
                    already_notified = False
                    self._event.clear()  # it should stop

                elif already_notified is False:
                    await self.AD.plugins.notify_plugin_stopped(self.name, self.namespace)
                    already_notified = True
                    self.logger.warning("Could not connect to the Database, will attempt in 5 seconds")

            except Exception as e:
                self.logger.error("-" * 60)
                self.logger.error(
                    "Could not setup connection to database %s", self._connection_url,
                )
                self.logger.error("-" * 60)
                self.logger.error(e)
                self.logger.debug(traceback.format_exc())
                self.logger.error("-" * 60)
                self._client = None

            await asyncio.sleep(5)

    async def event_callback(self, event, data, kwargs):
        self.logger.debug("event_callback: %s %s %s", kwargs, event, data)

        _namespace = kwargs["__namespace"]
        entity_id = data["entity_id"]

        if not await self.check_entity_id(_namespace, entity_id):
            return

        if data["new_state"]["state"] == data["old_state"].get("state"):
            # nothing changed
            return

        bucket = self._namespaces[_namespace]["bucket"]  # get the databases in this namespace
        tags = self._namespaces[_namespace]["tags"]
        state = data["new_state"]["state"]

        try:
            state = float(state)
        except Exception:
            if state in CONST_TRUE_STATES:
                state = 1.0
            elif state in CONST_FALSE_STATES:
                state = 0.0
            else:
                self.logger.warning(
                    f"Could not map {state} for {entity_id} Entity_ID to any valid data, and so will be ignored"
                )
                return

        attributes = data["new_state"]["attributes"]
        friendly_name = attributes["friendly_name"]
        domain, _ = entity_id.split(".")
        lc = data["new_state"].get("last_changed")

        if lc is None:
            last_changed = await self.AD.sched.get_now()
        else:
            last_changed = iso8601.parse_date(lc)

        write_tags = {"entity_id": entity_id}
        for tag in tags:
            if tag in attributes:
                write_tags[tag] = attributes[tag]

        fields = {domain: state}
        if self.stopping is False:
            asyncio.create_task(
                self.database_write(
                    bucket, measurement=friendly_name, tags=write_tags, fields=fields, timestamp=last_changed
                )
            )

    #
    # Service Call
    #

    async def call_plugin_service(self, namespace, domain, service, kwargs):
        self.logger.debug(
            "call_plugin_service() namespace=%s domain=%s service=%s kwargs=%s", namespace, domain, service, kwargs,
        )
        res = None

        bucket = kwargs.pop("bucket", self._bucket)

        if bucket is None:
            raise ValueError("Bucket must be given to execute the service call %s", service)

        if service == "write":
            asyncio.create_task(self.database_write(bucket, **kwargs))

        elif service == "read":
            res = await self.database_read(bucket, **kwargs)

        elif service == "get_history":
            return await self.get_history(**kwargs)

        return res

    async def database_write(self, bucket, **kwargs):
        """Used to execute a database query"""

        executed = False
        measurement = kwargs.get("measurement")
        tags = kwargs.get("tags")
        fields = kwargs.get("fields")
        ts = kwargs.get("timestamp", await self.AD.sched.get_now())

        try:

            write_data = {}
            if measurement is not None:
                write_data["measurement"] = measurement

            if isinstance(tags, dict):
                write_data["tags"] = tags

            if isinstance(fields, dict):
                write_data["fields"] = fields

            write_data["time"] = ts
            await asyncio.wait_for(
                utils.run_in_executor(self, self._write_api.write, bucket, self._org, write_data), timeout=5
            )

        except Exception as e:
            self.logger.error("-" * 60)
            self.logger.error("Could not execute database write. %s %s", bucket, kwargs)
            self.logger.error("-" * 60)
            self.logger.error(e)
            self.logger.debug(traceback.format_exc())
            self.logger.error("-" * 60)

        return executed

    async def database_read(self, bucket, **kwargs):
        """Used to fetch data from a database"""

        res = []
        query = kwargs.get("query")
        params = kwargs.get("params")

        if query is not None and isinstance(params, dict):
            try:
                tables = await utils.run_in_executor(self, self._query_api.query, query, params=params)
                for table in tables:
                    for record in table.records:
                        res.append(record)

            except Exception as e:
                self.logger.error("-" * 60)
                self.logger.error("Could not execute database read for query %s", query)
                self.logger.error("-" * 60)
                self.logger.error(e)
                self.logger.debug(traceback.format_exc())
                self.logger.error("-" * 60)

        else:
            self.logger.warning("Could not execute Database Read, as Query and Params as Dictionay is needed")

        return res

    async def get_history(self, **kwargs):
        """Get the history of data from the database"""

        tables = None
        try:
            entity_id = kwargs.get("entity_id")
            bucket = kwargs.get("bucket", self._bucket)
            measurement = kwargs.get("measurement")
            field = kwargs.get("field")
            filter_tags = kwargs.get("filter_tags")
            query = kwargs.get("query")
            params = kwargs.get("params")

            if query is None or not isinstance(params, dict):
                # only run this if the query is not given

                # first process time interval of the request
                start_time, end_time = self.get_history_time(**kwargs)

                if bucket is None:
                    raise ValueError("The required bucket to be accessed must be given")

                params = {"_start": start_time, "_stop": end_time, "_desc": True}

                query = f"""
                        from(bucket:"{bucket}") |> range(start: _start, stop: _stop)
                        """

                if measurement is not None:
                    query = query + f'|> filter(fn: (r) => r["_measurement"] == "{measurement}")'

                if field is not None:
                    query = query + f'|> filter(fn: (r) => r["_field"] == "{field}")'

                if entity_id is not None:
                    # need to use entity_id as tag
                    params["_entity_id"] = entity_id
                    query = query + '|> filter(fn: (r) => r["entity_id"] == _entity_id)'

                if isinstance(filter_tags, dict):
                    read_tag = {}
                    for tag, value in filter_tags.items():
                        if not tag.startswith("_"):
                            _tag = f"_{tag}"
                        else:
                            _tag = tag

                        read_tag[_tag] = value
                        striped_tag = _tag.lstrip("_")
                        query = query + f'|> filter(fn: (r) => r["{striped_tag}"] == {_tag})'

                    # update the params
                    params.update(read_tag)

                # specify decending order by time
                query = query + '|> sort(columns: ["_time"], desc: _desc)'

            tables = await self.database_read(bucket, query=query, params=params)
        except Exception as e:
            self.logger.error("-" * 60)
            self.logger.error("Could not execute database read. %s %s", bucket, kwargs)
            self.logger.error("-" * 60)
            self.logger.error(e)
            self.logger.debug(traceback.format_exc())
            self.logger.error("-" * 60)

        return tables

    def get_history_time(self, **kwargs):

        days = kwargs.get("days", 1)
        start_time = kwargs.get("start_time")
        end_time = kwargs.get("end_time")

        if start_time is not None:
            if isinstance(start_time, str):
                start_time = utils.str_to_dt(start_time).replace(microsecond=0)
            elif isinstance(start_time, datetime.datetime):
                start_time = self.AD.tz.localize(start_time).replace(microsecond=0)
            else:
                raise ValueError("Invalid type for start time")

        if end_time is not None:
            if isinstance(end_time, str):
                end_time = utils.str_to_dt(end_time).replace(microsecond=0)
            elif isinstance(end_time, datetime.datetime):
                end_time = self.AD.tz.localize(end_time).replace(microsecond=0)
            else:
                raise ValueError("Invalid type for end time")

        if start_time is not None and end_time is None:
            end_time = start_time + timedelta(days=days)

        # if endtime is declared and start_time is not declared,
        # and days specified
        elif end_time is not None and start_time is None:
            start_time = end_time - timedelta(days=days)

        elif start_time is None and end_time is None:
            end_time = datetime.now()
            start_time = end_time - timedelta(days=days)

        return start_time, end_time

    async def check_entity_id(self, namespace, entity_id):
        """Check if to store the entity's data"""

        execute = True

        if self._databases[namespace] is None:
            # there is no filers used for the database
            pass

        elif "exclude_entities" in self._databases[namespace]:
            excluded_entities = self._databases[namespace]["exclude_entities"]

            if isinstance(excluded_entities, str):
                execute = self.wildcard_check(excluded_entities, entity_id)
                execute = not execute  # invert it

            elif isinstance(excluded_entities, list):
                for entity in excluded_entities:
                    execute = self.wildcard_check(entity, entity_id)
                    execute = not execute  # invert it

                    if execute is False:
                        break

        elif "include_entities" in self._databases[namespace]:
            execute = False
            included_entities = self._databases[namespace]["include_entities"]

            if isinstance(included_entities, str):
                execute = self.wildcard_check(included_entities, entity_id)

            elif isinstance(included_entities, list):
                for entity in included_entities:
                    execute = self.wildcard_check(entity, entity_id)

                    if execute is True:
                        break

        return execute

    def wildcard_check(self, wildcard, data):
        """Used to check for if the data is within the wildcard"""

        execute = False
        if wildcard == data:
            execute = True

        elif wildcard.endswith("*") and data.startswith(wildcard[:-1]):
            execute = True

        elif wildcard.startswith("*") and data.endswith(wildcard[1:]):
            execute = True

        return execute

    def get_namespace(self):
        return self.namespace

    @property
    def get_write_api(self):
        return self._write_api

    @property
    def get_query_api(self):
        return self._query_api
