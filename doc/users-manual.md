# Metrics monitor: user manual

## Running the job

Source code should be first compiled with Apache Maven.

```
$SPARK_HOME/bin/spark-submit \
	--master yarn \
	--class ch.cern.spark.metrics.Driver \
	spark-metrics-*.jar \
	<path_to_conf_file>
```

## Configuration

A basic configuration contains a metric source, one or more monitors and analysis result or notifications sink.

Note that configuration of defined metrics and monitors is dynamic, so it can be updated while running. This dynamic configuration is obtained from the configured "properties.source", which by default is configured to read the same configuration file.

Each defined metric, monitor and notificator must have a different ID.

```
checkpoint.dir = <path_to_store_stateful_data> (default: /tmp/)
spark.batch.time = <period like 1h, 3m or 45s> (default: 1m)

# Data for metrics that are not coming will expire 
data.expiration = <period like 1h, 3m or 45s> (default: 30m)

# Optional
properties.source.type = <properties_source_type> (default: "file" with path to this configuration file)
properties.source.expire = <period like 1h, 3m or 45s> (default: 1m)
properties.source.<other_confs> = <value>

# At least one source is mandatory
metrics.source.<metric-source-id-1>.type = <metric_source_type>
metrics.source.<metric-source-id-1>.<other_confs> = <value>
metrics.source.<metric-source-id-1>.schema.<configs at Metric schemas> = <values>
metrics.source.<metric-source-id-2>...
metrics.source.<metric-source-id-n>...

# Optional (dynamic, coming from properties.source)
metrics.schema.<schema-id-1>.sources = <source-ids>
metrics.schema.<schema-id-1>...
metrics.schema.<schema-id-2>...
metrics.schema.<schema-id-n>...

# Optional (dynamic, coming from properties.source)
metrics.define.<defined-metric-1>...
metrics.define.<defined-metric-2>...
metrics.define.<defined-metric-n>...

# Monitors (dynamic, coming from properties.source)
monitor.<monitor-id-1>.<confs>...
monitor.<monitor-id-2>.<confs>...
monitor.<monitor-id-n>.<confs>...

# At least one sink must be declared
results.sink.type = <analysis_results_sink_type>
results.sink.<other_confs> = <value>
notifications.sink.<sink-id>.type = <notifications_sink_type>
notifications.sink.<sink-id>.<other_confs> = <value>
```

An example of full configuration can be:

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
notifications.sink.elastic.type = elastic
notifications.sink.elastic.index = itdb_db-metric-notifications/log

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
# This monitor does not produce notifications

# Monitor percentage of DB usage of all instances
monitor.DBCPU.filter.attribute.$defined_metric = DBCPUUsagePercentage
monitor.DBCPU.analysis.type = fixed-threshold
monitor.DBCPU.analysis.error.upperbound = 800DBCPU
monitor.DBCPU.analysis.warn.upperbound  = 600
monitor.DBCPU.analysis.error.lowerbound = -1
monitor.DBCPU.tags.email = databases-team@cern.ch
# This monitor does not produce notifications

# Monitor all metrics (no filter)
monitor.all-seasonal.analysis.type = seasonal
monitor.all-seasonal.analysis.season = hour
monitor.all-seasonal.analysis.learning.ratio = 0.2
monitor.all-seasonal.analysis.error.ratio = 6
monitor.all-seasonal.analysis.warn.ratio = 3
monitor.all-seasonal.notificator.error-constant.type = constant
monitor.all-seasonal.notificator.error-constant.sinks = elastic
monitor.all-seasonal.notificator.error-constant.statuses = ERROR
monitor.all-seasonal.notificator.error-constant.period = 10m
monitor.all-seasonal.notificator.warn-constant.type = constant
monitor.all-seasonal.notificator.warn-constant.sinks = ALL
monitor.all-seasonal.notificator.warn-constant.statuses = WARNING
monitor.all-seasonal.notificator.warn-constant.period = 20m
```

## A Metric

A metric in this monitor has the following characteristics:

* Contains a set of key-value pairs which are considered attributes. Key and value are strings. These attributes should serve to differentiate between metrics. These attributes are set by the metric source and can change when defining new metrics.
* It is marked with a time stamp. Time stamp is set by the metric source and can change when defining new metrics.
* Contains a single value. Value could be of type float, string or boolean. Type is determined by the metric source. When defining new metrics, aggregation operations or equation defined in value determine the type of the new metric.

Independently of the type of value, values contains an attribute with name "source". It describes how the value was obtained, equation, errors, ...

## Define new metrics

```
metrics.define.<defined-metric-id>.value = <equation containing <variable-ids>> (default: <variable-id> if only one variable has been declared)
metrics.define.<defined-metric-id>.when = <ANY|BATCH|space separated list of metric variable-ids> (default: the first metric variable after sorting)
metrics.define.<defined-metric-id>.metrics.groupby = <not set/ALL/space separated attribute names> (default: not set)
# General filter for all metrics that update the variables (optional)
metrics.define.<defined-metric-id>.metrics.filter.expr = <predicate with () | & = !=>
metrics.define.<defined-metric-id>.metrics.filter.attribute.<attribute-name> = <value>
# Variable that represent an incoming metric
metrics.define.<defined-metric-id>.variables.<variable-id-1>.filter.expr = <predicate with () | & = !=>
metrics.define.<defined-metric-id>.variables.<variable-id-1>.filter.attribute.<attribute-name> = <value->
metrics.define.<defined-metric-id>.variables.<variable-id-1>.aggregate = <not set|sum|avg|weighted_avg|count|max|min|diff>
metrics.define.<defined-metric-id>.variables.<variable-id-1>.expire = <never|period like 1h, 3m or 45s> (default: 10m)
# Variable that represent a set of properties for an analysis (could serve to configure an analysis: properties_variable)
metrics.define.<defined-metric-id>.variables.<variable-id-2>.type = <analysis_type>
metrics.define.<defined-metric-id>.variables.<variable-id-2>.<analysis-conf-key-1> = <value-1>
metrics.define.<defined-metric-id>.variables.<variable-id-2>.<analysis-conf-key-n> = <value-n>
metrics.define.<defined-metric-id>.variables.<variable-id-n>...

# With different id, more metrics can be defined
```

New metrics can be defined. The value of these defined metrics is computed from an equation configured with the "value" parameter. This equation can have variables, these variables can represent incoming metrics or a set of properties for configuring a function. Values from several metrics can be aggregated in order to compute the value for the new metric. Equation supports grouping using (), and it applies the operator precedence and associativity rules. By default, in case only one variable is declared, value will return this variable. Equation will determine the type of the new defined metric.

Functions that can be used in the equation with expected types and the type they return:
* \+ float => float
* \- float => float
* float + float => float
* float - float => float
* float * float => float
* float / float => float
* float ^ float => float
* float > float => boolean
* float < float => boolean
* boolean && boolean => boolean
* boolean || boolean => boolean
* !boolean => boolean
* if_bool(boolean, boolean, boolean) => boolean
* if_float(boolean, float, float) => float
* if_string(boolean, string, string) => string
* any == any => boolean
* any != any => boolean
* abs(float) => float
* sqrt(float) => float
* sin(float) => float
* cos(float) => float
* tan(float) => float
* concat(string, string) => string
* trim(string) => string
* analysis(any, analysis_properties_variable) => "OK"|"WARNING"|"ERROR" (string)

Examples:

```
metrics.define.temperature_change.value = !shouldBeMonitored || analysis(temperature, ana_props) != "OK"
metrics.define.temperature_change.variables.shouldBeMonitored.filter.attribute.TYPE = "Monitoring enabled"
metrics.define.temperature_change.variables.temperature.filter.attribute.TYPE = "Temperature in Celsious"
metrics.define.temperature_change.variables.ana_props.type = recent
metrics.define.temperature_change.variables.ana_props.error.upperbound = true
metrics.define.temperature_change.variables.ana_props.error.lowerbound = true

metrics.define.directory_full.value = !shouldBeMonitored || (trim(dir) == "/tmp/") && (abs(used / capacity) > 0.8)
metrics.define.directory_full.metrics.filter.attribute.TYPE = DirReport
metrics.define.directory_full.variables.shouldBeMonitored.filter.attribute.$value_attribute = monitor_enable
metrics.define.directory_full.variables.dir.filter.attribute.$value_attribute = path
metrics.define.directory_full.variables.used.filter.attribute.$value_attribute = used_bytes
metrics.define.directory_full.variables.capacity.filter.attribute.$value_attribute = capacity_bytes
```

> TIP For debugging, values include a source attribute where equation result can be observed with a value like:
>```
># With errors
>!(var(shouldBeMonitored)=true)=false || ((trim(var(dir)=" /tmp/  ")="/tmp/" == "/tmp/")=true && (abs((var(used)=900.0 / var(capacity)={Error: no value for the last 10 minutes})={Error: in arguments})={Error: in arguments} > 0.8)={Error: in arguments})={Error: in arguments})={Error: in arguments}
>
># With successful computation
>!(var(shouldBeMonitored)=true)=false || ((trim(var(dir)=" /tmp/  ")="/tmp/" == "/tmp/")=true && (abs((var(used)=900.0 / var(capacity)=1000.0)=0.9)=0.9 > 0.8)=true)=true)=true
>```

The computation and further generation of a new metric will be trigger when the variables listed in the "when" parameter are updated. By default, a new metric is produced when the first (after sorting alphabetically by &lt;variable-id&gt;) declared variable is updated with a new value. Last value of the other variables will be used for the computation. You can set "when" to ANY, it will trigger the generation when any of the variables is updated. You can also set "when" to BATCH, so the generation will be triggered not by any variable updated but in every Spark Streaming batch.

> TIP for a defined metric which aggregates with count to return 0. 
> ``` 
> # Machines sent metric if running
> metrics.define.machines-running.variables.value.filter.attribute.TYPE = "running"
> metrics.define.machines-running.variables.value.aggregate = count
> ```
> If "when" parameter is set with variable names (default), a count 0 will never be produced because no metric will trigger its generation (no metrics coming, that's why count is 0). To solve this, there are two possible solutions. 
> 1. The defined metric is triggered by another variable which only serves to trigger the generation. If trigger metric (variable) does not come, no metric is generated.
> ``` 
> # to add
> metrics.define.machines-running.value = value
> metrics.define.machines-running.when = trigger
> metrics.define.machines-running.variables.trigger.filter.attribute.TYPE = "other"
> ``` 
> 2. "when" parameter is set to BATCH, so every Spark Streaming batch the computation and generation is triggered.
> ``` 
> # to add
> metrics.define.machines-running.when = BATCH
> ```

Metrics can be grouped by (e.g. machine) with the "metrics.groupby" parameter in order to apply the equation to a set of metrics. 
Group by can be set to ALL, then each metric will be treated independently. 
If group by is configured to ALL (or all attributes the metrics contain are listed) there is no attributes to differenciate metrics and aggregate them, so aggregation is done over the historical values coming from the metric.

You need to specify what the variables in your equation represent by declaring variables. Then, &lt;variable-id-X&gt; can be used in the equation. The type of a variable is determined by the aggregation operation, if no aggregation operation is applied, it can become any time in the equation.

Variables are supposed to be updated periodically. In case they are not updated, its value expires after the period of time specified with the parameter "expire". 
You can make variables to never expire configuring "expire" parameter to "never". By default, variables get expired after 10 minutes. 
If a variable expires and the variable is used for the computation, no metrics will be produced. For aggregations, individual values are removed from the aggregation if they are not updated after such period. 
In the case all the values for a given aggregated variable expire, count is 0.

A variable could be the result of an aggregation of values. Values from all metrics that pass the specified filter (and after grouping) will be aggregated. Note that in order to differentiate between metric, any attribute not specified in "groupby" will be used. 
This can be configured using the "aggregate" parameter, where you configure the operation to perform the aggregation. Operation determines the variable type. The maximum number of different metrics that can be aggregated is 100000, if more, oldest metrics are removed.

Aggregation operations available and the corresponding type:
* No operation => metric as generated by metric source
* sum => filter by float type and produce float
* avg => filter by float type and produce float
* weighted_avg (influence proportional with elapsed time) => filter by float type and produce float
* count_floats => filter by float type and produce float
* count_bools => filter by boolean type and produce float
* count_true => filter by boolean type and produce float
* count_false => filter by boolean type and produce float
* count_strings => filter by string type and produce float
* max => filter by float type and produce float
* min => filter by float type and produce float
* diff (with previous value) => filter by float type and produce float

A meta-attribute is set in the generated metrics. The meta attribute name is $defined_metric and his value the &lt;defined-metric-id&gt;. 
This attribute can later be used to filter the defined metrics in a monitor like:

```
monitor.<monitor_id>.filter.attribute.$defined_metric = <defined-metric-id>
```

Configuration of defined metrics can be updated while running.

Some examples of defined metrics can be:

- Multiply all metrics by 10

```
metrics.define.all-multiply-by-10.value = value * 10
metrics.define.all-multiply-by-10.metrics.groupby = ALL
# One of the following two would be enough
metrics.define.all-multiply-by-10.variables.value.filter.attribute.INSTANCE_NAME = .*
metrics.define.all-multiply-by-10.variables.value.filter.attribute.METRIC_NAME = .*
```

- Divide CPU usage coming from all machines by 1000

```
metrics.define.cpu-percentage = value / 1000
metrics.define.cpu-percentage.metrics.groupby = ALL
# Same effect if we specify INSTANCE_NAME = .* or not
#metrics.define.cpu-percentage.variables.value.filter.attribute.INSTANCE_NAME = .*
metrics.define.cpu-percentage.variables.value.filter.attribute.METRIC_NAME = CPU Usage Per Sec
```

- Compute the ratio read/write for all machines:

```
metrics.define.ratio_read_write.value = readbytes / writebytes
metrics.define.ratio_read_write.metrics.groupby = HOSTNAME
metrics.define.ratio_read_write.variables.readbytes.filter.attribute.METRIC_NAME = Read Bytes Per Sec
metrics.define.ratio_read_write.variables.writebytes.filter.attribute.METRIC_NAME = Write Bytes Per Sec
```

- Temperature inside minus temperature outside in Fahrenheits: 

```
metrics.define.diff_temp.value = (tempinside - tempoutside) * 9/5 + 32
# We do not group by, so that we can aggregate any metrics
metrics.define.diff_temp.variables.tempinside.filter.attribute.PLACE = Living Room
metrics.define.diff_temp.variables.tempinside.filter.attribute.METRIC = Temperature
metrics.define.diff_temp.variables.tempoutside.filter.attribute.PLACE = Outside
metrics.define.diff_temp.variables.tempoutside.filter.attribute.METRIC = Temperature
``` 

- Compare values of production and development environments:

```
metrics.define.diff-prod-dev.value = valueprod - valuedev
metrics.define.diff-prod-dev.metrics.groupby = INSTANCE_NAME METRIC_NAME
# Metrics contain $source attribute with <metric-source-id>, it can be used to filter
metrics.define.diff-prod-dev.variables.valueprod.filter.attribute.$source = kafka-prod
metrics.define.diff-prod-dev.variables.valuedev.filter.attribute.$source = kafka-dev
```

- Aggregate metrics for all machines for each cluster and environment

All metrics that belongs to the same cluster will be averaged. They will be grouped by METRIC_NAME.
Metrics coming from the same HOSTNAME, will update its previous value in the aggregation.

``` 
metrics.define.avg-metric-per-cluster.metrics.groupby = CLUSTER_NAME METRIC_NAME
metrics.define.avg-metric-per-cluster.variables.average-value.aggregate = avg
```

- Aggregate metrics for all machines in production for each cluster

All metrics that belongs to the same cluster (groupby), name is "Read Bytes" and environment is "production" will be accumulated.
Metrics coming from the same HOSTNAME, will update its previous value in the aggregation.

``` 
metrics.define.clusterprod-read-bytes.metrics.groupby = CLUSTER_NAME
metrics.define.clusterprod-read-bytes.variables.readbytes.filter.attribute.ENVIRONMENT = production
metrics.define.clusterprod-read-bytes.variables.readbytes.filter.attribute.METRIC_NAME = Read Bytes
metrics.define.clusterprod-read-bytes.variables.readbytes.aggregate = sum
# Value parameter is optional since there is only one variable and it has the desired value
metrics.define.clusterprod-read-bytes.value = readbytes
```

- Count number of machines running per cluster

Scenario: a machine that is running, produce a metric of type "running". If the machine stops, no metric are sent.

If the machine do not send the metric after 5 minutes, its corresponding metric is removed from the aggregation. 

``` 
metrics.define.cluster-machines-running.metrics.groupby = CLUSTER_NAME
metrics.define.cluster-machines-running.when = BATCH
metrics.define.cluster-machines-running.variables.value.filter.attribute.TYPE = "running"
metrics.define.cluster-machines-running.variables.value.aggregate = count
metrics.define.cluster-machines-running.variables.value.expire = 5m
```

- Average value from each metric during the period of the last 5 minutes 

If we group by all the attributes (groupby = ALL), each metric is treated independently. 

Under such circunstancies there is no attrbutes in the metric to differenciate metrics and aggregate them, so aggregation is done over the historical values of the metric.

``` 
metrics.define.avg-5m.metrics.groupby = ALL
metrics.define.avg-5m.variables.value.aggregate = avg
metrics.define.avg-5m.variables.value.expire = 5m
```

- Detect if a metric stop coming (missing metric)

If we group by all the attributes (groupby = ALL), each metric will be treated independently. 

We can count how many metrics arrived in the last 10 minutes. With a monitor we can check if this defined metric gets 0, that would mean the metric is not coming.

``` 
metrics.define.missing-metric.metrics.groupby = ALL
metrics.define.missing-metric.when = BATCH
metrics.define.missing-metric.variables.value.aggregate = count
metrics.define.missing-metric.variables.value.expire = 10m
```

### Metric filters

The filter determine the rules a metric must pass in order to accept the metric.

It acts on the attributes of the metrics. Only configured attributes are checked.

For "attribute" parameters, you can negate the condition by placing "!" as first character in the value. That would mean: attribute should not be the specified value or should not match the regular expression.

It can specify a regular expression or an exact value for the attribute:
```
filter.attribute.<attribute_key> = <[!]regex_or_exact_value>
```

Metrics can be filtered by metric source:
```
filter.attribute.$source = <metric-source-id>
```

They can also be filtered by defined metric:
```
filter.attribute.$defined_metric = <defined-metric-id>
```

More complex filter can be configured using the "expr" parameter. Regular expressions can be used.

```
filter.expr = <predicate with () | & = !=>
```

You can combine "expr" and "attribute" parameters, all attribute parameters are "and" predicates with "expr".

An example:

```
# CLUSTER must be "cluster1"
# and HOST must be "host1" or "host2"
# and NOT_VALID must not be defined
filter.expr = "CLUSTER = \"cluster1\" & (HOST = 'host1' | HOST='host2') & NOT_VALID != .*"
# Optionally you can specify more conditions
# and $source must be kafka
filter.attribute.$source = kafka
```

### Monitors

```
## filter (optional)
monitor.<monitor-id>.filter.expr = <predicate with () | & = !=>
monitor.<monitor-id>.filter.attribute.<metric_attribute_key> = <[!]regex_or_exact_value>
monitor.<monitor-id>.filter.attribute... (as many attributes as needed)
## analysis
monitor.<monitor-id>.analysis.type = <analysis_type>
monitor.<monitor-id>.analysis.<other_confs> = <value>
## notificators (optional)
monitor.<monitor-id>.notificator.<notificator-id-1>.type = <notificator-type>
monitor.<monitor-id>.notificator.<notificator-id-1>.sinks = <ALL|space separated notificatios sink ids> (default: ALL)
monitor.<monitor-id>.notificator.<notificator-id-1>.tags.<tag-key-1> = <value-1>
monitor.<monitor-id>.notificator.<notificator-id-1>.tags.<tag-key-2> = <value-2>
monitor.<monitor-id>.notificator.<notificator-id-1>.tags.<tag-key-n> = <value-n>
monitor.<monitor-id>.notificator.<notificator-id-1>.<other_confs> = <value>
monitor.<monitor-id>.notificator.<notificator-id-n>... (as many notificators as needed)
monitor.<monitor-id>.tags.<tag-key-1> = <value-1>
monitor.<monitor-id>.tags.<tag-key-2> = <value-2>
monitor.<monitor-id>.tags.<tag-key-n> = <value-n>
```

Configuration of monitors can be updated while running.

#### Tags

Each monitor or notificator can have different tags that are included in the analysis results and notifications that the monitor produces.

They could be used to later discriminate the data, to aggregate, to target notifications (email, group, system), configure sinks, ...

Monitor tags are included in the analysis results. They can be configured as:

```
monitor.<monitor-id>.tags.<tag-key-1> = <value-1>
monitor.<monitor-id>.tags.<tag-key-2> = <value-2>
monitor.<monitor-id>.tags.<tag-key-n> = <value-n>
```

Tags at monitor level are override by notificator tags. Merged tags from monitor and notificator are included in notifications.

```
monitor.<monitor-id>.notificator.<notificator-id>.tags.<tag-key> = <value>
```

Tags used for configuring sinks are removed from the notifications.

The value of any tag can be extracted from an attribute of the analyzed metric. For that you should follow the syntax:

```
<tag-key-1> = %<metric-key>
```

## Components

For any of the components, type must be specified. Type can be any of the built-in components or a FQCN of an external component.

### Properties sources

Components which configuration can be updated while running, defined metrics and monitors, obtain their configuration from this source.

This source will be periodically queried, every "expire" period, and the job will be updated with the new configuration.

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
properties.source.path = {path to this file}
```

#### File properties source

This source obtains all properties from a text file with format readable by java.util.Properties.

```
properties.source.type = file
properties.source.path = <path_to_configuration_file>
```

### Metric sources

For any metrics source, a metrics schema can be configured.

```
metrics.source.<metric-source-id-n>.schema.<configs at JSON to Metric schema> = <values>
```

Metric sources produces JSON documents which represent metrics. These JSON documents are later interpreted by metric schemas.

#### Kafka metric source

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

#### Schemas for metrics

Schemas serve to interpret the output (JSON) of a Metrics source and convert it to metrics. 

Each source needs at least one schema. More than one schema can be defined to a source and one schema can be applied to one or more sources.

Metrics generated by each schema will have an attribute with key "$schema" and value the ID of the schema.

There are two ways of defining a schema:
* At the source configuration: in case all the data comes with the same structure you just apply one schema in the source. 
Schema ID does not need to be specified. Metric will contain an $schema attribute with the same ID as the source.
This configuration cannot be updated while running.
* Later in the configuration file with metrics.schema.schema-id prefix: in case the source consumes metrics with different schemas. 
You can defined several schemas for the same source (for each of the incoming metric types). 
If same type/schema come from several of your sources, you can apply an schema to several of them.
This configuration can be updated while running.   

In any case, you can apply a metrics filter. This way you make sure that the schema is applied to the metrics that the schema should be concerned of (a certain incoming type).
NOTE: filter is applied after parsing, so it can only be applied to the attributes that are parsed in the schema. If you want to filter on attributes that are not parsed, you need to parse them. 

For both cases, the configuration parameters are the following:

```
metrics.schema.<schema-id>.sources = <space-separated-source-ids>
metrics.schema.<schema-id>.timestamp.key = <attribute that represent the time>
metrics.schema.<schema-id>.timestamp.format = <timestamp_format> (default: yyyy-MM-dd'T'HH:mm:ssZ)
metrics.schema.<schema-id>.attributes = <attributs separated by comma to extract from the JSON>
metrics.schema.<schema-id>.attributes.<alias-1> = <key-to-attribute-1>
metrics.schema.<schema-id>.attributes.<alias-2> = <key-to-attribute-2>
metrics.schema.<schema-id>.attributes.<alias-n> = <key-to-attribute-n>
metrics.schema.<schema-id>.value.keys = <attributes that represent values>
metrics.schema.<schema-id>.value.keys.<alias-1> = <key-to-value-1>
metrics.schema.<schema-id>.value.keys.<alias-2> = <key-to-value-2>
metrics.schema.<schema-id>.value.keys.<alias-n> = <key-to-value-n>
metrics.schema.<schema-id>.filter.<configs at Metrics filter> = <values>
```

For configuring the schema in the source, you replace metrics.schema.schema-id by metrics.source.source-id.schema.

"timestamp.key" indicates the key in the JSON document that contains the timestamp for the metric. If the JSON document does not contain the timestamp value, no metric will be generated. 

"timestamp.format" indicates the format of the timestamp stored in the attribute configured by "timestamp.attribute". If the format is a number that represents epoch in milliseconds, it must be set to "epoch-ms", if seconds "epoch-s". If the JSON document contains a timestamp with wrong format, metric with exception value will be generated, setting the timestamp to current time. 

"attributes" configure the keys that will be extracted from the JSON document. You can indicate a list of keys separated by space.
You can also configure these attributes individually assigning aliases. Assigned alias will be used to refer to the attribute in any metric filter. 
List of keys or aliases can be combined.
If the JSON document does not contain the attribute, the metric will not contain such attribute.

"value.keys" configure the keys from which metric values will be extracted from the JSON document. You can indicate a list of keys separated by space. A metric will be created for each key, all metrics generated from the same JSON document will share the same timestamp and attributes. All generated metrics will contain an extra attribute with name "$value\_attribute", its value indicates the key (or alias) from which the value has been extracted. 
You can also configure these attributes for values individually assigning aliases. Assigned alias will be stored at "$value\_attribute", so the alias will be used in any metric filter.
List of keys or aliases can be combined.
If JSON document does not contain the value, the metric will not be generated.
Type of generated metrics will be determined by the corresponding JSON type, string float or boolean.

Commonly, you can face two different scenarios. One where each incoming JSON represent a single metric, an example of JSON document could be: 

```
{
	"headers":{
		"TIMESTAMP": "2017-11-01T10:29:14+02:00",
		"type": "CPUUsage",
		"hostname": "host-1234.cern.ch"
	},"body":{
		"metric":{
			"value": 295.13
		}
	}
}
```

For this kind of documents, the configuration would looks like the following.

```
metrics.schema.<schema-id>.timestamp.key = header.TIMESTAMP

metrics.schema.<schema-id>.attributes = headers.type headers.hostname
# or/and with aliases
metrics.schema.<schema-id>.attributes.type = headers.type
metrics.schema.<schema-id>.attributes.hostname = headers.hostname

metrics.schema.<schema-id>.value.keys = body.metric.value
# or/and with aliases
metrics.schema.<schema-id>.value.keys.value_float = body.metric.value
```

One metric will be generated per JSON document. The metric timestamp will be "2017-11-01T10:29:14+02:00" and the value 295.13. If not using aliases, metric will have the following attributes:
* headers.type = "CPUUsage"
* headers.hostname = "host-1234.cern.ch"
* $value\_attribute = "body.metric.value"

If aliases are used:
* type = "CPUUsage"
* hostname = "host-1234.cern.ch"
* $value\_attribute = "value\_float"

A different scenario is when a single document contains several metrics, an example of JSON document could be:

```
{
	"headers":{
		"TIMESTAMP": "2017-11-01T10:29:14+02:00",
		"hostname": "host-1234.cern.ch"
	},"body":{
		"CPUUsage": 295.13,
		"IsWriteActive": true,
		"CPUName": "Intel Core i8"
	}
}
```

For this kind of documents, the configuration would looks like the following.

```
metrics.schema.<schema-id>.timestamp.keys = header.TIMESTAMP

metrics.schema.<schema-id>.attributes = headers.hostname
# or/and with aliases
metrics.schema.<schema-id>.attributes.hostname = headers.hostname

metrics.schema.<schema-id>.value.keys = body.CPUUsage body.MemoryUsage body.WriteBytesPerSecond body.ReadBytesPerSecond
# or/and with aliases
metrics.schema.<schema-id>.value.keys.CPUUsage = body.CPUUsage
metrics.schema.<schema-id>.value.keys.WriteBytesPerSecond = body.WriteBytesPerSecond
metrics.schema.<schema-id>.value.keys.ReadBytesPerSecond = body.ReadBytesPerSecond
```

In this case, three metrics will be generated per JSON document (as many as attributes for values). Generated metrics will be (if aliases are not used):
* Metric with timestamp "2017-11-01T10:29:14+02:00", value 295.13 and attributes:
  * headers.hostname = "host-1234.cern.ch"
  * $value\_attribute = "body.CPUUsage"
* Metric with timestamp "2017-11-01T10:29:14+02:00", value true and attributes:
  * headers.hostname = "host-1234.cern.ch"
  * $value\_attribute = "body.IsWriteActive"
* Metric with timestamp "2017-11-01T10:29:14+02:00", value "Intel Core i8" and attributes:
  * headers.hostname = "host-1234.cern.ch"
  * $value\_attribute = "body.CPUName"

If aliases are used:
* Metric with timestamp "2017-11-01T10:29:14+02:00", value 295.13 and attributes:
  * hostname = "host-1234.cern.ch"
  * $value\_attribute = "CPUUsage"
* Metric with timestamp "2017-11-01T10:29:14+02:00", value true and attributes:
  * hostname = "host-1234.cern.ch"
  * $value\_attribute = "IsWriteActive"
* Metric with timestamp "2017-11-01T10:29:14+02:00", value "Intel Core i8" and attributes:
  * hostname = "host-1234.cern.ch"
  * $value\_attribute = "CPUName"

### Metric analysis

Each metric analysis works only with a type metric values.

#### Always true analysis

Filter metrics with boolean values. 

If incoming metric is true, analysis result is OK, otherwise ERROR.

Configuration:
```
monitor.<monitor-id>.analysis.type = true
```

#### Fixed thresholds analysis

Filter metrics with float values. 

Fixed values determine the status of the metric. 
If metric goes upper or lower these values, corresponding status (warning or error) is produced. Otherwise, ok status is produced.

Configuration:
```
monitor.<monitor-id>.analysis.type = fixed-threshold
monitor.<monitor-id>.analysis.error.upperbound = <value>
monitor.<monitor-id>.analysis.warn.upperbound  = <value>
monitor.<monitor-id>.analysis.warn.lowerbound  = <value>
monitor.<monitor-id>.analysis.error.lowerbound = <value>
```

An example of the result of this analysis can be seen in the following image.
![Fixed thresholds analysis](img/analysis/fixed-thresholds.png)

#### Recent activity analysis 

Filter metrics with float values.

Error and warning thresholds are computed using average and variance from previos period.

- Upper error threshold is computed as: mean + variance * error.ratio
- Upper warning threshold is computed as: mean + variance * warn.ratio
- Lower warning threshold is computed as: mean - variance * warn.ratio
- Lower error threshold is computed as: mean - variance * error.ratio

Configuration:
```
monitor.<monitor-id>.analysis.type = recent
monitor.<monitor-id>.analysis.period = <period like 1h, 3m or 45s> (default: 5m)
monitor.<monitor-id>.analysis.error.ratio = <float> (dafault: 1.8)
monitor.<monitor-id>.analysis.warn.ratio = <float> (dafault: 1.5)
# Each threshold can be activated by:
monitor.<monitor-id>.analysis.error.upperbound = <true|false> (default: false)
monitor.<monitor-id>.analysis.warn.upperbound  = <true|false> (default: false)
monitor.<monitor-id>.analysis.warn.lowerbound  = <true|false> (default: false)
monitor.<monitor-id>.analysis.error.lowerbound = <true|false> (default: false)
```

An example of the result of this analysis can be seen in the following image.
![Recent activity analysis](img/analysis/recent.png)

#### Percentile analysis

Filter metrics with float values.

Error and warning thresholds are computed using percentiles from previos period.

- Upper error threshold is computed as: percentile(error.percentile) + diff(percentile - median) * error.ratio
- Upper warning threshold is computed as: percentile(warn.percentile) + diff(percentile - median) * warn.ratio
- Lower warning threshold is computed as: percentile(100 - warn.percentile) - diff(percentile - median) * warn.ratio
- Lower error threshold is computed as: percentile(100 - error.percentile) - diff(percentile - median) * error.ratio

Configuration:
```
monitor.<monitor-id>.analysis.type = percentile
monitor.<monitor-id>.analysis.period = <period like 1h, 3m or 45s> (default: 5m)
monitor.<monitor-id>.analysis.error.percentile = <50-100> (dafault: 99)
monitor.<monitor-id>.analysis.error.ratio = <float> (dafault: 0.3)
monitor.<monitor-id>.analysis.warn.percentile = <50-100> (dafault: 98)
monitor.<monitor-id>.analysis.warn.ratio = <float> (dafault: 0.2)
# Each threshold can be activated by:
monitor.<monitor-id>.analysis.error.upperbound = <true|false> (default: false)
monitor.<monitor-id>.analysis.warn.upperbound  = <true|false> (default: false)
monitor.<monitor-id>.analysis.warn.lowerbound  = <true|false> (default: false)
monitor.<monitor-id>.analysis.error.lowerbound = <true|false> (default: false)
```

An example of the result of this analysis can be seen in the following image.
![Percentile analysis](img/analysis/percentile.png)

#### Seasonal analysis

Filter metrics with float values.

Metric is supposed to behave similarly in every season. Saeson can be hour, day or week.

Using a learning coeficient, average and variance are computed along the season for every minute, these two values are used to calculate error and warning thresholds.

- Upper error threshold is computed as: mean + standDev * error.ratio
- Upper warning threshold is computed as: mean + standDev * warn.ratio
- Lower warning threshold is computed as: mean - standDev * warn.ratio
- Lower error threshold is computed as: mean - standDev * error.ratio

Configuration:
```
monitor.<monitor-id>.analysis.type = seasonal
monitor.<monitor-id>.analysis.season = <hour, day or week> (default: hour)
monitor.<monitor-id>.analysis.learning.ratio = <float> (default: 0.5)
monitor.<monitor-id>.analysis.error.ratio = <float> (default: 4)
monitor.<monitor-id>.analysis.warn.ratio = <float> (default: 2)
```  

An example of the result of this analysis can be seen in the following image.
![Seasonal analysis](img/analysis/seasonal.png)

### Analysis results sinks

#### Elastic Analysis results sink

Analysis results are converted to JSON and sunk to an Elastic index.

```
results.sink.type = elastic
results.sink.index = <index>
spark.es.nodes=<nodes>
spark.es.port=<port>
spark.es.<any_other_attribute> = <value>
```

#### HTTP Analysis results sink

Analysis results are converted to JSON (array) and sunk to an HTTP (POST) end point.

```
results.sink.type = http
results.sink.url = <url>
results.sink.parallelization = <number-of-parallel-clients> (default: 5)
results.sink.batch.size = <max-number-of-records-in-a-POST-request> (default: 100)
results.sink.retries = <max-number-of-retries> (default: 1)
results.sink.timeout = <max-wait-time-in-ms> (default: 2000)
# HTTP simple authentication
results.sink.auth = <true|false> (default: false)
results.sink.auth.user = <username>
results.sink.auth.password = <password>
# Add properties to JSON document
results.sink.add.<key-1> = <value-1|%analysis-result-tag-key>
results.sink.add.<key-2> = <value-2|%analysis-result-tag-key>
results.sink.add.<key-n> = <value-n|%analysis-result-tag-key>
```

### Notificators

All notificators have at least the following parameters:

```
monitor.<monitor-id>.notificator.<notificator-id>.type = <type>
monitor.<monitor-id>.notificator.<notificator-id>.sinks = <ALL|notifications-sinks-ids> (default: ALL)
monitor.<monitor-id>.notificator.<notificator-id>.tags.<tag-key> = <value>
```

#### Statuses notificator

If a metric arrives in any of the configured statuses, it produces a notification.

Possible statuses are: error, warning, ok, exception.

Configuration:
```
monitor.<monitor-id>.notificator.<notificator-id>.type = statuses
monitor.<monitor-id>.notificator.<notificator-id>.statuses = <concerned statuses separated by space>
monitor.<monitor-id>.notificator.<notificator-id>.silent.period = <period like 1h, 3m or 45s> (default: 0)
```

Minimum period between two notifications is ".silent.period".

#### Constant status notificator

If a metric has been in configured statuses during the configured period, it produces a notification.

Possible statuses are: error, warning, ok, exception.

Configuration:
```
monitor.<monitor-id>.notificator.<notificator-id>.type = constant
monitor.<monitor-id>.notificator.<notificator-id>.statuses = <concerned statuses separated by space>
monitor.<monitor-id>.notificator.<notificator-id>.period = <period like 1h, 3m or 45s> (default: 15m)
monitor.<monitor-id>.notificator.<notificator-id>.silent.period = <period like 1h, 3m or 45s> (default: 0)
```

Minimum period between two notifications is ".period" plus ".silent.period".

An example of the result of this notificator can be seen in the following image.
![Constant status notificator](img/notificator/constant.png)

#### Percentage status notificator

If a metric has been in configured statuses during a percentage of the configured period, it produces a notification.

Possible statuses are: error, warning, ok, exception.

Configuration:
```
monitor.<monitor-id>.notificator.<notificator-id>.type = percentage
monitor.<monitor-id>.notificator.<notificator-id>.sinks = <ALL|notifications-sinks-ids> (default: ALL)
monitor.<monitor-id>.notificator.<notificator-id>.statuses = <concerned statuses separated by space>
monitor.<monitor-id>.notificator.<notificator-id>.period = <period like 1h, 3m or 45s> (default: 15m)
monitor.<monitor-id>.notificator.<notificator-id>.silent.period = <period like 1h, 3m or 45s> (default: 0)
monitor.<monitor-id>.notificator.<notificator-id>.percentage = <0-100> (default: 90)
```

Minimum period between two notifications is ".period" plus ".silent.period".

An example of the result of this notificator can be seen in the following image.
![Percentage status notificator](img/notificator/percentage.png)

### Notifications sinks

#### Elastic notifications sink

Notifications are converted to JSON and sunk to an Elastic index.

```
notifications.sink.<sink-id>.type = elastic
notifications.sink.<sink-id>.index = <index>
spark.es.nodes=<nodes>
spark.es.port=<port>
spark.es.<any_other_attribute> = <value>
```

#### HTTP notifications sink

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
* body.metadata.uuid = (randomly generated UUID [+info](https://docs.oracle.com/javase/7/docs/api/java/util/UUID.html#randomUUID())

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



