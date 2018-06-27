# Properties sources

Configuration can be updated while running (except sources), configuration is obtained from this component.

This source will be periodically queried, every "expire" period, and ExDeMon will be updated with the new configuration.

To configure an external source of properties:

```
properties.source.type = <properties_source_type>
properties.source.expire = <period like 1h, 3m or 45s> (default: 1m)
properties.source.id.filters.<filter_name1> = <regex>
properties.source.id.filters.<filter_nameN> = <regex>
properties.source.static.<key1> = <value>
properties.source.static.<keyn> = <value> 
properties.source.<other_confs> = <value>
```

Component's properties to be loaded will be filtered by id.filters. All these regexs will build filter for ids with or operator.

If not properties source is configured, the default configuration is:

```
properties.source.type = file
properties.source.expire = 1m
properties.source.path = {path to main configuration file}
```

## Zookeeper properties source

This source keeps properties synchronized with Zookeeper. 

It expects JSON documents describing the configuration in nodes with name equal to "conf_node_name". The expected nodes structure is:
```
/type=<component_type>/id=<component_id_1>/<name_of_nodes_with_config>
/type=<component_type>/id=<component_id_2>/<name_of_nodes_with_config>
...
```

Where component_type can be one of: schema, metric, monitor or actuator.

Configuration:

```
properties.source.type = zookeeper
properties.source.connection_string = <host:port,host:port/path>
properties.source.initialization_timeout_ms = <milliseconds> (default: 5000)
properties.source.timeout_ms = <milliseconds> (default: 20000)
properties.source.conf_node_name = <name_of_nodes_with_config> (default: config)
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

## File properties source

This source obtains all properties from a file or set of files.

The format of the text files must be readable by java.util.Properties or a JSON that can be converted to java.util.Properties.

```
properties.source.type = file
properties.source.path = <path_to_configuration_file_or_directory>
```