# Properties sources

Configuration can be updated while running (except sources), configuration is obtained from this component.

This source will be periodically queried, every "expire" period, and ExDeMon will be updated with the new configuration.

To configure an external source of properties:

```
properties.source.type = <properties_source_type>
properties.source.expire = <period like 1h, 3m or 45s> (default: 1m)
properties.source.id.filters.<filter_name1> = <regex>
properties.source.id.filters.<filter_nameN> = <regex> 
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

Configuration:

```
properties.source.type = zookeeper
properties.source.connection_string = <host:port,host:port/path>
properties.source.initialization_timeout_ms = <milliseconds> (default: 5000)
properties.source.timeout_ms = <milliseconds> (default: 20000)
properties.source.asjson = <node_with_json_name> (recommended)
```

If asjson property is not specified, it expects the following structure in the specified path:

```
/type=schema/id=spark_batch/attributes/$environment = qa
/type=schema/id=perf/timestamp/key = data.timestamp
/type=schema/id=perf/timestamp/format = YYYY-MM-DD HH:MM:SS
```

If asjson property is specified, it expects that nodes in Zookeeper with the configured name contains JSON (recommended). 
Configured with asjson=json_text, It expects:

```
/type=schema/id=spark_batch/json_text = { "qa": "$environment" }
/type=schema/id=perf/json_text = { "timestamp": { "key": "data.timestamp", "format": "YYYY-MM-DD HH:MM:SS" }}
```

Both would be translated to:

```
metrics.schema.spark_batch.attributes.$environment = qa
metrics.schema.perf.timestamp.key = data.timestamp
metrics.schema.perf.timestamp.format = YYYY-MM-DD HH:MM:SS
```

## File properties source

This source obtains all properties from a file or set of files.

The format of the text files must be readable by java.util.Properties or a JSON that can be converted to java.util.Properties.

```
properties.source.type = file
properties.source.path = <path_to_configuration_file_or_directory>
```