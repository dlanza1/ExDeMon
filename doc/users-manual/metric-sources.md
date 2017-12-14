# Metric sources

Metric sources produces JSON documents which represent metrics. These JSON documents are later interpreted by metric schemas.

Optionally, a metrics schema can be configured in the source. This metric schema cannot be updated once the application is running.

```
metrics.source.<metric-source-id-n>.schema.<configs at JSON to Metric schema> = <values>
```

## Kafka metric source

It expects documents as JSON.

Configuration:

```
metrics.source.<source-id>.type = kafka
metrics.source.<source-id>.topics = <consumer topic>
metrics.source.<source-id>.consumer.bootstrap.servers = <bootstrap_servers separated by comma>
metrics.source.<source-id>.consumer.group.id = <consumer_group_id>
# All these parameters (source.consumer.) will by passed to the consumer
metrics.source.<source-id>.consumer.<any_other_key> = <any_other_value>
metrics.source.<source-id>.schema.<configs at metric schema definition section> = <values>
```