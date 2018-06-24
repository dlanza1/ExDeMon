# Metric filters

The filter determine the rules a metric must pass in order to accept the metric.

You can specify that metrics with timestamp older than a period (comparing with current time), should be filtered out.
```
filter.timestamp.expire = <period like 1h, 3m or 45s> (default: not set)
```

For "attribute" parameters, you can negate the condition by placing "!" as first character in the value. That would mean: attribute should not be the specified value or should not match the regular expression.

You can specify a regular expression or an exact value for the attribute:
```
filter.attribute.<attribute_key> = [!]<regex_or_exact_value>
```

You can also specify multi-values between quotes (! at the beginning would negate all):
```
filter.attribute.<attribute_key> = [!]"<regex_or_exact_value>" "<regex_or_exact_value>" "<regex_or_exact_value>" ...
```

You can also filter by some meta-attributes:
```
filter.attribute.$source = <metric-source-id>
filter.attribute.$schema = <metric-schema-id>
filter.attribute.$defined_metric = <defined-metric-id>
filter.attribute.$monitor = <monitor-id>
```

More complex filter can be configured using the "expr" parameter. Regular expressions can be used.

```
filter.expr = <predicate with () | & = !=>
```

You can combine "expr" and "attribute" parameters, all attribute parameters are "and" predicates with "expr".

An example:

```
# No metrics generated more than 24h ago
filter.timestamp.expire = 24h
# CLUSTER must be "cluster1"
# and HOST must be "host1" or "host2"
# and NOT_VALID must not be defined
filter.expr = "CLUSTER = \"cluster1\" & (HOST = 'host1' | HOST='host2') & NOT_VALID != .*"
# Optionally you can specify more conditions
# and $source must be kafka
filter.attribute.$source = kafka
```
