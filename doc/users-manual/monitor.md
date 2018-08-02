# Monitors

Monitor is the abstraction used in this application to determine what [metrics](metrics.md) should be monitored, how and when to trigger actions.

Each monitor have:
* Optionally, a [filter](metrics-filter.md) to determine what metrics should be monitor. If not filter is applied, all metrics will be processed.
* An [analysis](monitor-analysis.md) to determine the status of each metric. Default analysis always results in status OK.
* Zero or more [triggers](monitor-triggers.md), they determine when to raise actions.

The configuration parameters can be seen below.

```
## filter (optional)
monitor.<monitor-id>.filter.expr = <predicate with () | & = !=>
monitor.<monitor-id>.filter.attribute.<metric_attribute_key> = <[!]regex_or_exact_value>
monitor.<monitor-id>.filter.attribute... (as many attributes as needed)
# Fixed value attributes to add to the generated analyzed metric
monitor.<monitor-id>.attribute.<attribute-name> = <value>
## analysis
monitor.<monitor-id>.analysis.type = <analysis_type> (default: none analysis)
monitor.<monitor-id>.analysis.<other_confs> = <value>
## triggers (optional)
monitor.<monitor-id>.triggers.<trigger-id-1>.type = <trigger-type>
monitor.<monitor-id>.triggers.<trigger-id-1>.actuators = <ALL|space separated actuators ids> (default: ALL)
monitor.<monitor-id>.triggers.<trigger-id-1>.tags.<tag-key-1> = <value-1>
monitor.<monitor-id>.triggers.<trigger-id-1>.tags.<tag-key-2> = <value-2>
monitor.<monitor-id>.triggers.<trigger-id-1>.tags.<tag-key-n> = <value-n>
monitor.<monitor-id>.triggers.<trigger-id-1>.<other_confs> = <value>
monitor.<monitor-id>.triggers.<trigger-id-n>... (as many triggers as needed)
monitor.<monitor-id>.triggers.$error.type = <trigger-type> (default: constant)
monitor.<monitor-id>.triggers.$error.sinks = <sinks> (default: ALL)
monitor.<monitor-id>.triggers.$error.statuses = EXCEPTION
monitor.<monitor-id>.triggers.$error.period = <period> (default: 10m)
monitor.<monitor-id>.tags.<tag-key-1> = <value-1>
monitor.<monitor-id>.tags.<tag-key-2> = <value-2>
monitor.<monitor-id>.tags.<tag-key-n> = <value-n>
```

Configuration of monitors can be updated while running.

## Tags

Each monitor or trigger can have different tags that are included in the analysis results and actions that the monitor produces.

They could be used to later discriminate the data, to aggregate, to target notifications (email, group, system), configure sinks, ...

Monitor tags are included in the analysis results. They can be configured as:

```
monitor.<monitor-id>.tags.<tag-key-1> = <value-1>
monitor.<monitor-id>.tags.<tag-key-2> = <value-2>
monitor.<monitor-id>.tags.<tag-key-n> = <value-n>
```

Tags at monitor level are override by trigger tags. Merged tags from monitor and trigger are included in actions.

```
monitor.<monitor-id>.triggers.<trigger-id>.tags.<tag-key> = <value>
```

Tags used for configuring actuators are removed from the actions.

The value of any tag can be extracted from an attribute of the analyzed metric. For that you should follow the syntax:

```
<tag-key-1> = %<metric-key>
```

## Status

In order to perform the analysis, previous activity needs to be stored. This is stored in a status.

You may want to list the current statuses in order to understand the analysis results or remove the status of, for example, a specific host.

For that, please read the [statuses management documentation](statuses-management.md).

