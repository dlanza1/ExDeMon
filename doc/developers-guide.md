# Spark Streaming job for monitoring metrics: developers guide

## Components

External components can be developed by extending the corresponding classes (look at component type below for details).

Internal components should annotate the class with @RegisterComponent("<name>"), specifying a name for referring to it in the configuration. 

Any component can override the config() method. Properties parameter will contain only the corresponding configuration for the component.

### Store for stateful components

Analysis and notificators may need to keep some historical data. If so, implemented component can implement the interface ch.cern.spark.metrics.store.HasStore.

The save() method must return an object which implemeants the interface Store and contains only the data that needs to be stored.

The load() method receives the previous saved store.

Data contained in Store will be serialized and may thousands of these are stored. Taking into account that, the Store should contain as less data as possible.

### Properties source 

This component is meant to produce a set of properties that will be merged with the properties from the configuration file.
Properties from the configuration file cannot be overwritten.

Externally developed properties sources must extend ch.cern.PropertiesSource.

### Metric source

This component is meant to consume metrics from a source and generate an stream of metrics. 

Externally developed sources must extend ch.cern.spark.metrics.source.MetricsSource.

### Metric analysis

This component is meant to determine the status (error, warning, exception, ok) of each of the incoming metrics.  

Externally developed analysis must extend ch.cern.spark.metrics.analysis.Analysis.

If same data need to be kept, this component can make use of a [Store](#store-for-stateful-components).

### Analysis results sink

This component produce results for each of the incoming metrics. These results can be sinked to an external storage for watching the metric and analysis results.

Externally developed analysis results sinks must extend ch.cern.spark.metrics.results.sink.AnalysisResultsSink.

### Notificator

This component determines when to raise a notifications based on analysis results.

Externally developed notificators must extend ch.cern.spark.metrics.notificator.Notificator.

If same data need to be kept, this component can make use of a [Store](#store-for-stateful-components).

### Notifications sink

Notifications produced by notificators are sink using this component. Notifications can be sinked to an external storage, sent by email, used to trigger actions, etc. 

Externally developed analysis results sinks must extend ch.cern.spark.metrics.results.sink.AnalysisResultsSink.