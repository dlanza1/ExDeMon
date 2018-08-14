# Metrics monitor: user manual

Let's build a basic understanding of how the tool works.

1) Metrics are consumed from different systems by using [metric sources](metric-sources.md). Metric sources produce JSON documents that represent metrics.
2) JSON documents are interpreted by [metric schemas](metrics-schema.md). The result are [metrics](metrics.md) that can be used in the application. Each metric source should have associated at least a metric schema. One metric schema can be associated to several sources.
3) New [metrics can be defined](define-metrics.md) based on incoming metrics.
4) Metrics coming from schemas or from defined metrics are consumed by [monitors](monitor.md).
5) Monitors [filter](metrics-filter.md), [analyze](monitor-analysis.md) and [trigger actions](monitor-triggers.md).
6) Analysis results can be sunk to an external service by using an [analysis results sink](analysis-results-sink.md).
7) [Silences](silences.md) may drop actions. Use case: interventions on services.
8) Triggered actions are processed by [actuators](actuators.md).

An image that describes some of the previous concepts and shows the data flow can be seen here.
  
![Data flow](/doc/img/dataflow.png)

### Index

* [Components source](components-source.md)
* [Metrics source](metric-sources.md)
* [Metric schemas](metrics-schema.md)
* [Definition of new metrics](define-metrics.md)
* [Monitor](monitor.md)
  * [Filter](metrics-filter.md) 
  * [Analysis](monitor-analysis.md)
  * [Triggers](monitor-triggers.md)
* [Analysis results sink](analysis-results-sink.md)
* [Silences](silences.md)
* [Actuators](actuators.md)

* [Statuses management](statuses-management.md)

## Configuration

A basic configuration contains a [metrics source](metric-sources.md), one or more [monitors](monitor.md) and [analysis results sink](analysis-results-sink.md) or [actuators](actuators.md).

Note that the configuration is dynamic (except for sources), so it can be updated while running. Dynamic components are obtained from the configured [components source](components-source.md), which by default is configured to read the same configuration file.

Any ID in the configuration must follow the regular expression: "[a-zA-Z0-9_-]+"

The general structure of the configuration file is shown below.

```
checkpoint.dir = <path_to_store_stateful_data> (default: /tmp/)
spark.batch.time = <period like 1h, 3m or 45s> (default: 1m)

# Optional
components.source.type = <properties_source_type> (default: "file" with path to this configuration file)
components.source.expire = <period like 1h, 3m or 45s> (default: 1m)
components.source.<other_confs> = <value>

# Optional
# +info at components that store statuses: defined metrics, monitors and triggers
statuses.removal.socket = <host:port>

# Default statuses store
spark.cern.streaming.status.storage.type = single-file
spark.cern.streaming.status.storage.path = /tmp/metrics-monitor-statuses/

# At least one source is mandatory
metrics.source.<metric-source-id-1>.type = <metric_source_type>
metrics.source.<metric-source-id-1>.<other_confs> = <value>
metrics.source.<metric-source-id-1>.schema.<configs at Metric schemas> = <values>
metrics.source.<metric-source-id-2>...
metrics.source.<metric-source-id-n>...

# Optional (dynamic, coming from components.source)
metrics.schema.<schema-id-1>.sources = <source-ids>
metrics.schema.<schema-id-1>...
metrics.schema.<schema-id-2>...
metrics.schema.<schema-id-n>...

# Optional (dynamic, coming from components.source)
metrics.define.<defined-metric-1>...
metrics.define.<defined-metric-2>...
metrics.define.<defined-metric-n>...

# Monitors (dynamic, coming from components.source)
monitor.<monitor-id-1>.<confs>...
monitor.<monitor-id-2>.<confs>...
monitor.<monitor-id-n>.<confs>...

# At least one sink must be declared
results.sink.type = <analysis_results_sink_type>
results.sink.<other_confs> = <value>
actuators.<sink-id>.type = <a_type>
actuators.<sink-id>.<other_confs> = <value>
```

### Example of full configuration can be:

```
checkpoint.dir = /tmp/spark-metrics-job/

# Metric comes from a Kafka cluster
metrics.source.kafka-prod.type = kafka
metrics.source.kafka-prod.consumer.bootstrap.servers = habench101.cern.ch:9092,habench102.cern.ch:9092,habench103.cern.ch:9092
metrics.source.kafka-prod.consumer.group.id = spark_metric_analyzer
metrics.source.kafka-prod.topics = db-logging-platform
# These two parameters are extracted from metrics (they are enough to identify a metric)
metrics.source.kafka-prod.schema.attributes = INSTANCE_NAME METRIC_NAME
metrics.source.kafka-prod.schema.value.attributes = VALUE
metrics.source.kafka-prod.schema.timestamp.attribute = END_TIME

metrics.define.DBCPUUsagePercentage.value = DBCPUUsagePerSec / HostCPUUsagePerSec
metrics.define.DBCPUUsagePercentage.metrics.groupby = INSTANCE_NAME
metrics.define.DBCPUUsagePercentage.variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME = CPU Usage Per Sec
metrics.define.DBCPUUsagePercentage.variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME = Host CPU Usage Per 

metrics.define.cluster-total-read-bytes.metrics.groupby = CLUSTER_NAME
metrics.define.cluster-total-read-bytes.variables.readbytes.filter.attribute.METRIC_NAME = Read Bytes
metrics.define.cluster-total-read-bytes.variables.readbytes.aggregate = sum

# Analysis results are sinked to Elastic
results.sink.type = elastic
results.sink.index = itdb_db-metric-results/log

# Notifications are sinked to Elastic
actuators.elastic.type = elastic
actuators.elastic.index = itdb_db-metric-notifications/log

spark.es.nodes=es-itdb.cern.ch
spark.es.port=9203

# Monitor CPU of all instances
# attribute.INSTANCE_NAME does not need to be specified, same effect as .*
# filter can be configured with attributes or expr (for more complex conditions)
monitor.CPUUsage.filter.expr = INSTANCE_NAME=.* & METRIC_NAME="CPU Usage Per Sec"
monitor.CPUUsage.filter.attribute.INSTANCE_NAME = .*
monitor.CPUUsage.filter.attribute.METRIC_NAME = CPU Usage Per Sec
monitor.CPUUsage.analysis.type = fixed-threshold
monitor.CPUUsage.analysis.error.upperbound = 800
monitor.CPUUsage.analysis.warn.upperbound  = 600
monitor.CPUUsage.analysis.error.lowerbound = -1
monitor.CPUUsage.tags.email = procurement-team@cern.ch
# This monitor does not trigger actions

# Monitor percentage of DB usage of all instances
monitor.DBCPU.filter.attribute.$defined_metric = DBCPUUsagePercentage
monitor.DBCPU.analysis.type = fixed-threshold
monitor.DBCPU.analysis.error.upperbound = 800DBCPU
monitor.DBCPU.analysis.warn.upperbound  = 600
monitor.DBCPU.analysis.error.lowerbound = -1
monitor.DBCPU.tags.email = databases-team@cern.ch
# This monitor does not trigger actions

# Monitor all metrics (no filter)
monitor.all-seasonal.analysis.type = seasonal
monitor.all-seasonal.analysis.season = hour
monitor.all-seasonal.analysis.learning.ratio = 0.2
monitor.all-seasonal.analysis.error.ratio = 6
monitor.all-seasonal.analysis.warn.ratio = 3
monitor.all-seasonal.triggers.error-constant.type = constant
monitor.all-seasonal.triggers.error-constant.actuators = elastic
monitor.all-seasonal.triggers.error-constant.statuses = ERROR
monitor.all-seasonal.triggers.error-constant.period = 10m
monitor.all-seasonal.triggers.warn-constant.type = constant
monitor.all-seasonal.triggers.warn-constant.actuators = ALL
monitor.all-seasonal.triggers.warn-constant.statuses = WARNING
monitor.all-seasonal.triggers.warn-constant.period = 20m
```

## Starting

Once the configuration has been done, you can refer to the section [running the application](running.md).
