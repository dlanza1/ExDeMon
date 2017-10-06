# Spark Streaming job for monitoring metrics

This Spark Streaming job is desinged for monitoring metrics. 

[User's manual](doc/users-manual.md)

### Key feautres

- Several monitors can be declared, each monitor can have a metric filter, a metric pre-analysis, a metric analysis and notificators. 
- Only a metric source, analysis result sink and notifications sink can be declared. They are shared by all monitors. 
- Components: metrics source, pre-analysis, analysis, analysis results sink, notificator and notification sink. They can be replaced. 
- Some built-in components: Kafka source, different pre-analysis and analyis, Elastic sink, notificators, ...
- Metrics at different frequencies.
- Monitors configuration can be updated while running.
- Detection of missing metrics.

## Monitors

Metrics are consumed by a source and sent to all monitors.

Many monitors can be declared. Each monitor has a filter to determine to which metrics it should be applied.
Filtered metrics are pre-analyzed if a pre-analysis is configured. Once pre-analyzed, an analysis is applied to determine the current status of the metric.
Several notificators can be configured to produce notifications.

Results from analysis and notifications can be sinked to an external storage.

## Components

They are considered parts of the processing pipeline that can be easily replaced by other built-in components or by an externally developed component.

If you are willing to develop any component, look at the [developers guide](doc/developers-guide.md).

### Metric source

This componenet is ment to consume metrics from a source and generate an stream of metrics. 

Only one source is declared for the job. All monitors consume from this source.

Built-in metric sources:
- Kafka.

### Metric pre-analysis

This componenet is ment to transform incoming metrics before the analysis. If a pre-analysis is applied, the produced value will be the value used by the analysis.

Each monitor configures or not its own pre-analysis.

Built-in metric pre-analysis:
- Average value during certain period.
- Weighted average value during certain period. The closer in time the metri is to current time, the more influence is has over analyzed value.
- Difference: difference with previous value.

### Metric analysis

This component is ment to determine the status (error, warning, exception, ok) of each of the incoming metrics (pre-analyzed or not).

Each monitor configures or not its own analysis.

Built-in metric analysis:
- Fixed threshold: error and warning thresholds.
- Recent activity: error and warning thresholds are computed using average and variance from recent activity.
- Percentile: error and warning thresholds are computed based on percentiles from recent activity.
- Seasonal: a season is configured (hour, day or week), using a learning coeficient, average and variance are computed along the season, these two values are used to calculate error and warning thresholds.  

### Analysis results sink

Analysis produce results for each of the incoming metrics. These results can be sinked to an external storage for watching the metric and analysis results.

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

Notifications produced by notificators are sink using this componenet. Notifications can be sinked to an external storage, sent by email, used to trigger actions, etc.

Only one notifications sink is declared for the job. All monitors use this sink.

Built-in notifications sink:
- Elastic.