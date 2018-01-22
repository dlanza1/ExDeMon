# Notifications sinks

Notifications produced by [notificators](monitor-notificator.md) are sunk by this component to an external system.

#### Templates

In some sinks, templates can be used to format notifications.

The following strings will be replaced by the notification corresponding information:
* <monitor_id>
* <notificator_id>
* <metric_attributes> (will be replaced by a list)
* <metric_attributes:key> (will be replaced by the value of the specified attribute key)
* <datetime>
* <reason>
* <tags> (will be replaced by the tag list)
* <tags:key> (will be replaced by the value of the specified tag key)

## Elastic notifications sink

Notifications are converted to JSON and sunk to an Elastic index.

```
notifications.sink.<sink-id>.type = elastic
notifications.sink.<sink-id>.index = <index>
spark.es.nodes=<nodes>
spark.es.port=<port>
spark.es.<any_other_attribute> = <value>
```

## Email notifications sink

Notifications are sent by email.

```
notifications.sink.<sink-id>.type = email
notifications.sink.<sink-id>.session.mail.smtp.host = <email-server>
notifications.sink.<sink-id>.session.mail.smtp.auth = <true|false>
notifications.sink.<sink-id>.session.<others> = <value>
notifications.sink.<sink-id>.username = <from-email>
notifications.sink.<sink-id>.password = <from-email-password>
# Email info
notifications.sink.<sink-id>.to = <value|%notification-tag-key> (default: %email.to)
notifications.sink.<sink-id>.subject = <value|%notification-tag-key> (default: %email.subject)
notifications.sink.<sink-id>.text = <value|%notification-tag-key> (default: %email.text)
```

text and subject can be templates.

## HTTP notifications sink

Notifications are converted to JSON (array) and sunk to an HTTP (POST) end point.

```
notifications.sink.<sink-id>.type = http
notifications.sink.<sink-id>.url = <url>
notifications.sink.<sink-id>.parallelization = <number-of-parallel-clients> (default: 1)
notifications.sink.<sink-id>.batch.size = <max-number-of-records-in-a-POST-request> (default: 100)
notifications.sink.<sink-id>.as-array = <true|false> (default: true)
notifications.sink.<sink-id>.retries = <max-number-of-retries> (default: 5)
notifications.sink.<sink-id>.timeout = <max-wait-time-in-ms> (default: 2000)
# HTTP simple authentication
notifications.sink.<sink-id>.auth = <true|false> (default: false)
notifications.sink.<sink-id>.auth.user = <username>
notifications.sink.<sink-id>.auth.password = <password>
# Add properties to JSON document
notifications.sink.<sink-id>.add.$notification = <true|false> (default: true)
notifications.sink.<sink-id>.add.<key-1> = <value|%notification-tag-key>
notifications.sink.<sink-id>.add.<key-2> = <value|%notification-tag-key>
notifications.sink.<sink-id>.add.<key-n> = <value|%notification-tag-key>
```

add parameters can use templates.

TIP: Send notifications to Mattermost

More info: https://docs.mattermost.com/developer/webhooks-incoming.html

```
notifications.sink.mattermost.type = http
notifications.sink.mattermost.url = http://{your-mattermost-site}/hooks/xxx-generatedkey-xxx
notifications.sink.mattermost.add.$notification = false
notifications.sink.mattermost.add.text = %matt-text
notifications.sink.mattermost.add.channel = %matt-channel
notifications.sink.mattermost.add.username = %matt-username
notifications.sink.mattermost.add.icon_url = %matt-icon_url
```

## CERN GNI notifications sink

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
