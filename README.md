# Spark Streaming job for monitoring metrics

A general purpose metric monitor using Apache Spark. 
Metrics coming from Kafka or any other source, results and notifications can be sunk to Elastic or any other system, new metrics can be defined combining other metrics, different analysis can be applied, notifications, configuration can be updated without restarting, it can detect missing metrics, ...

[User's manual](doc/users-manual.md)

An example of a monitored metric can be observed in the following image. As it can be observed, thresholds around the value are calculated and statuses are generated if analyzed value exceed these limits. 
Notifications can be raised if certain statuses like error or warning are maintained during some time.    

![Example of monitored metric](/doc/img/example-monitored-metric.png)

### Key features

- New metrics can be defined. Mathematical operations can be applied. Value of new metrics can be computed by aggregating different incoming metrics. 
- Several monitors can be declared, each monitor can have a metric filter, a metric pre-analysis, a metric analysis and notificators. 
- Only a metric source, analysis result sink and notifications sink can be declared. They are shared by all monitors. 
- Components: properties source, metrics source, pre-analysis, analysis, analysis results sink, notificator and notification sink. They can be replaced. 
- Some built-in components: Kafka source, different pre-analysis and analysis, Elastic sink, notificators, ...
- Metrics at different frequencies.
- Monitors and defined metrics configuration can be updated while running. Configuration could come from an external source (Apache Zookeeper, HTTP request, data base, ...).
- Detection of missing metrics.

An image that describes some of the previous concepts and shows the data flow in the streaming job can be seen here.  
![Data flow](/doc/img/data-flow.png)

## Define new metrics

The value of these defined metrics is computed from a mathematical equation which is configured. 

This equation can have or not variables, these variables represent incoming metrics. So, values from several metrics can be aggregated in order to compute the value for the new metric.

Metrics can be grouped by (e.g. machine) in order to apply the equation to a set of metrics.

Some possibilities of defined metrics could be:
- Multiply all metrics by 10: value * 100
- Compute the ratio read/write for all machines: (groupby: hostname) readbytes / writebytes
- Temperature inside minus temperature outside: tempinside - tempoutside 

## Monitors

Metrics are consumed by a source and sent to all monitors.

Many monitors can be declared. Each monitor has a filter to determine to which metrics it should be applied.
Filtered metrics are pre-analyzed if a pre-analysis is configured. Once pre-analyzed, an analysis is applied to determine the current status of the metric.
Several notificators can be configured to produce notifications.

Results from analysis and notifications can be sunk to an external storage.

## Components

They are considered parts of the processing pipeline that can be easily replaced by other built-in components or by an externally developed component.

If you are willing to develop any component, look at the [developers guide](doc/developers-guide.md).

### Properties source

This component is meant to consume configuration properties from an external source.

This source will be continuously queried and the job will be updated with coming properties.

### Metric source

This component is meant to consume metrics from a source and generate an stream of metrics. 

Only one source is declared for the job. All monitors consume from this source.

Built-in metric sources:
- Kafka.

### Metric pre-analysis

This component is meant to transform incoming metrics before the analysis. If a pre-analysis is applied, the produced value will be the value used by the analysis.

Each monitor configures or not its own pre-analysis.

Built-in metric pre-analysis:
- Average value during certain period.
- Weighted average value during certain period. The closer in time the metric is to current time, the more influence it has over analyzed value.
- Difference: difference with previous value.

### Metric analysis

This component is meant to determine the status (error, warning, exception, ok) of each of the incoming metrics (pre-analyzed or not).

Each monitor configures or not its own analysis.

Built-in metric analysis:
- Fixed threshold: error and warning thresholds.
- Recent activity: error and warning thresholds are computed using average and variance from recent activity.
- Percentile: error and warning thresholds are computed based on percentiles from recent activity.
- Seasonal: a season is configured (hour, day or week), using a learning coefficient, average and variance are computed along the season, these two values are used to calculate error and warning thresholds.  

### Analysis results sink

Analysis produce results for each of the incoming metrics. These results can be sunk to an external storage for watching the metric and analysis results.

Only one analysis results sink is declared for the job. All monitors use this sink.

Built-in analysis results sink:
- Elastic.

### Notificator

A notificator determine when to raise a notifications based on analysis results.

Several notificators can be configured in a monitor.

Built-in notificators:
- Constant status: if a metric has been in configured statuses during a certain period.
- Percentage status: if a metric has been in configured statuses during a percentage of a certain period.

### Notifications sink

Notifications produced by notificators are sunk using this component. Notifications can be sunk to an external storage, sent by email, used to trigger actions, etc.

Only one notifications sink is declared for the job. All monitors use this sink.

Built-in notifications sink:
- Elastic.