# Metric filters

The filter determine the rules a metric must pass in order to accept the metric.

It acts on the attributes of the metrics. Only configured attributes are checked.

For "attribute" parameters, you can negate the condition by placing "!" as first character in the value. That would mean: attribute should not be the specified value or should not match the regular expression.

It can specify a regular expression or an exact value for the attribute:
```
filter.attribute.<attribute_key> = <[!]regex_or_exact_value>
```

You can also filter by meta-attributes:
```
filter.attribute.$source = <metric-source-id>
filter.attribute.$schema = <metric-schema-id>
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
