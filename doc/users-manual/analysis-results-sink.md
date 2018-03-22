# Analysis results sinks

Using this component, results coming from metrics analysis are send to an external system.

It can be useful to understand why an action is trigger, or use the results of aggregations for other purposes.

## Elastic Analysis results sink

Analysis results are converted to JSON and sunk to an Elastic index.

```
results.sink.type = elastic
results.sink.index = <index>
spark.es.nodes=<nodes>
spark.es.port=<port>
spark.es.<any_other_attribute> = <value>
```

## HTTP Analysis results sink

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
results.sink.add.<key-3> = [keys:key_regex] (array of keys from JSON document that match specified regex)
results.sink.add.<key-n> = <value-n|%analysis-result-tag-key>
```