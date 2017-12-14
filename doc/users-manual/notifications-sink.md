# Notifications sinks

Notifications produced by [notificators](monitor-notificator.md) are sunk by this component to an external system.

## Elastic notifications sink

Notifications are converted to JSON and sunk to an Elastic index.

```
notifications.sink.<sink-id>.type = elastic
notifications.sink.<sink-id>.index = <index>
spark.es.nodes=<nodes>
spark.es.port=<port>
spark.es.<any_other_attribute> = <value>
```

## HTTP notifications sink

Notifications are converted to JSON (array) and sunk to an HTTP (POST) end point.

```
notifications.sink.<sink-id>.type = http
notifications.sink.<sink-id>.url = <url>
notifications.sink.<sink-id>.parallelization = <number-of-parallel-clients> (default: 1)
notifications.sink.<sink-id>.batch.size = <max-number-of-records-in-a-POST-request> (default: 100)
notifications.sink.<sink-id>.retries = <max-number-of-retries> (default: 5)
notifications.sink.<sink-id>.timeout = <max-wait-time-in-ms> (default: 2000)
# HTTP simple authentication
notifications.sink.<sink-id>.auth = <true|false> (default: false)
notifications.sink.<sink-id>.auth.user = <username>
notifications.sink.<sink-id>.auth.password = <password>
# Add properties to JSON document
notifications.sink.<sink-id>.add.<key-1> = <value|%notification-tag-key>
notifications.sink.<sink-id>.add.<key-2> = <value|%notification-tag-key>
notifications.sink.<sink-id>.add.<key-n> = <value|%notification-tag-key>
```

#### CERN GNI notifications sink

Notifications are converted to JSON (array) and sunk to an HTTP (POST) end point.

You can find all possible fields at: https://itmon.web.cern.ch/itmon/data_types/notifications_specification.html

Some fields are set by default but could be override. Default values:
* header.m_version = "2"
* header.m_type = "notification"
* body.metadata.timestamp = (current epoch seconds)
* body.metadata.uuid = (randomly generated UUID [+info](https://docs.oracle.com/javase/7/docs/api/java/util/UUID.html#randomUUID() )

Integer fields will be parsed properly.

```
notifications.sink.<sink-id>.type = cern-gni
notifications.sink.<sink-id>.url = <url>
notifications.sink.<sink-id>.parallelization = <number-of-parallel-clients> (default: 1)
notifications.sink.<sink-id>.batch.size = <max-number-of-records-in-a-POST-request> (default: 100)
notifications.sink.<sink-id>.retries = <max-number-of-retries> (default: 5)
notifications.sink.<sink-id>.timeout = <max-wait-time-in-ms> (default: 2000)
notifications.sink.<sink-id>.content.header.<header-key> = <value|%notification-tag-key>
notifications.sink.<sink-id>.content.body.metadata.<metadata-key> = <value|%notification-tag-key>
notifications.sink.<sink-id>.content.body.payload.<payload-key> = <value|%notification-tag-key>
```
