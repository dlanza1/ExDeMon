# Properties sources

Configuration can be updated while running (except sources), configuration is obtained from this component.

This source will be periodically queried, every "expire" period, and ExDeMon will be updated with the new configuration.

To configure an external source of properties:

```
properties.source.type = <properties_source_type>
properties.source.expire = <period like 1h, 3m or 45s> (default: 1m)
properties.source.<other_confs> = <value>
```

If not properties source is configured, the default configuration is:

```
properties.source.type = file
properties.source.expire = 1m
properties.source.path = {path to main configuration file}
```

## File properties source

This source obtains all properties from a file or set of files.

The format of the text files must be readable by java.util.Properties or a JSON that can be converted to java.util.Properties.

```
properties.source.type = file
properties.source.path = <path_to_configuration_file_or_directory>
```