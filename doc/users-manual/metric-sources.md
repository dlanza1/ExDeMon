# Metric sources

Metric sources produces JSON documents. These JSON documents are later interpreted by metric schemas to be parsed to metrics.

Optionally, generated stream can be repartitioned with "partitions" parameter. By default, no repartitioning is done.

```
metrics.source.<metric-source-id-n>.partitions = <number-of-partitions>
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
```