# Metrics

Metric schemas produces metrics. A metric in ExDeMon has the following characteristics:

* Contains a set of key-value pairs which are considered attributes. Key and value are always strings. These attributes should serve to differentiate between metrics. These attributes are set by the metric source and can change when defining new metrics.
* It is marked with a time stamp. Time stamp is extracted by metric schemas.
* Contains a single value. Value could be of type float, string or boolean. Type is determined by the metric schema. When defining new metrics, aggregation operations or equation defined in value determine the type of the new metric.