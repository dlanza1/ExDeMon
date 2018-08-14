# Components sources

Configuration can be updated while running (except sources), configuration is obtained from this component.

To configure an external source of properties:

```
components.source.type = <properties_source_type>
components.source.id.filters.<filter_name1> = <regex>
components.source.id.filters.<filter_nameN> = <regex>
components.source.static.<key1> = <value>
components.source.static.<keyn> = <value> 
components.source.<other_confs> = <value>
```

Component's properties to be loaded will be filtered by id.filters. All these regexs will build filter for ids with "or" operator.

If not properties source is configured, the default configuration is:

```
components.source.type = file
components.source.expire = 1m
components.source.path = {path to main configuration file}
```

## Zookeeper components source

This source keeps components synchronized with Zookeeper. 

It expects JSON documents describing the configuration in nodes with name equal to "conf_node_name". The expected nodes structure is:
```
/type=<component_type>/id=<component_id_1>/<name_of_nodes_with_config>
/type=<component_type>/id=<component_id_2>/<name_of_nodes_with_config>
...
```

Where component_type can be one of: schema, metric, monitor or actuator.

Configuration:

```
components.source.type = zookeeper
components.source.connection_string = <host:port,host:port/path>
components.source.initialization_timeout_ms = <milliseconds> (default: 5000)
components.source.timeout_ms = <milliseconds> (default: 20000)
components.source.conf_node_name = <name_of_nodes_with_config> (default: config)
```

An example of structure if "conf_node_name" is configured to the default value (config).

```
/type=schema/id=storage_logs/config
/type=monitor/id=overused/config
/type=actuator/id=api/config
```

Where config nodes must contain a JSON document. Example of configuration for HTTP actuator:

```
{
  "type": "http",
  "url": "http://monit-logs.cern.ch:10012/",
  "add": {
    "producer": "tape",
    "type": "metrics-monitor",
    "phase": "notifications"
  }
}
```

## File components source

This source obtains the configuration from a file or set of files.

The format of the text files must be readable by java.util.Properties or a JSON that can be converted to java.util.Properties.

```
components.source.type = file
components.source.expire = <period like 1m, 1h> (default: 1m)
components.source.path = <path_to_configuration_file_or_directory>
```

It expects the following structure in the file:

```
# Optional (dynamic, coming from components.source)
metrics.schema.<id>...
metrics.define.<id>...
monitor.<id>...
actuators.<id>...
```