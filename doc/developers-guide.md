# Spark Streaming job for monitoring metrics: developers guide

## Components

External components can be developed by extending the corresponding classes (look at component type below for details).

Internal components should annotate the class with @RegisterComponent("<name>"), specifying a name for referring to it in the configuration. 

Any component can override the config() method. Properties parameter will contain only the corresponding configuration for the component.

### State for stateful components

Defined metrics, analysis and triggers may need to keep some historical data. If so, the component can implement the interface ch.cern.spark.status.HasState.

The save() method must return an object which implemeants the interface StateValue and contains only the data that needs to be stored.

The load() method receives the previous saved status.

Data contained in Status will be serialized and may thousands of these are stored. Taking into account that, the Status should contain as less data as possible.

### Properties source 

This component is meant to produce a set of properties that will be merged with the properties from the configuration file.
Properties from the configuration file cannot be overwritten.

Externally developed properties sources must extend ch.cern.PropertiesSource.

### Metric source

This component is meant to consume metrics from a source and generate an stream of metrics. 

If the source consume event as JSON, it is encouraged the usage of a common parser: ch.cern.spark.json.JSONObjectToMetricParser.

Externally developed sources must extend ch.cern.spark.metrics.source.MetricsSource.

### Metric analysis

This component is meant to determine the status (error, warning, exception, ok) of each of the incoming metrics.  

Externally developed analysis must extend ch.cern.spark.metrics.analysis.Analysis.

If same data need to be kept, this component can have a [Status](#store-for-stateful-components).

### Analysis results sink

This component produce results for each of the incoming metrics. These results can be sinked to an external storage for watching the metric and analysis results.

Externally developed analysis results sinks must extend ch.cern.spark.metrics.results.sink.AnalysisResultsSink.

### Triggers

This component determines when to raise an action based on analysis results.

Externally developed triggers must extend ch.cern.spark.metrics.trigger.Trigger.

If same data need to be kept, this component can have a [Status](#store-for-stateful-components).

### Actuators

Actions produced by triggers are processed using this component. Actions can be sinked to an external storage, sent by email, used to run jobs, etc. 

Externally developed actuators must extend ch.cern.spark.metrics.trigger.action.actuator.Actuator.