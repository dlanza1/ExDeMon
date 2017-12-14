# Metrics

Metric schemas produces metrics. A metric in this monitor has the following characteristics:

* Contains a set of key-value pairs which are considered attributes. Key and value are strings. These attributes should serve to differentiate between metrics. These attributes are set by the metric source and can change when defining new metrics.
* It is marked with a time stamp. Time stamp is set by the metric source and can change when defining new metrics.
* Contains a single value. Value could be of type float, string or boolean. Type is determined by the metric source. When defining new metrics, aggregation operations or equation defined in value determine the type of the new metric.

Independently of the type of value, values contains an attribute with name "source". It describes how the value was obtained, equation, errors, ...