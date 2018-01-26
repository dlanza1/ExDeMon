# Monitor: notificators

A notificator implement the logic to decide when to raise a notification base on the status of the metric computed by the previous analysis.

All notificators have at least the following parameters:

```
monitor.<monitor-id>.notificator.<notificator-id>.type = <type> (default: statuses)
monitor.<monitor-id>.notificator.<notificator-id>.filter.attribute.<analyzed_metric_attribute_key> = <[!]regex_or_exact_value>
monitor.<monitor-id>.notificator.<notificator-id>.filter.attribute... (as many attributes as needed)
monitor.<monitor-id>.notificator.<notificator-id>.sinks = <ALL|notifications-sinks-ids> (default: ALL)
monitor.<monitor-id>.notificator.<notificator-id>.tags.<tag-key> = <value>
```

## Statuses notificator

If a metric arrives in any of the configured statuses, it produces a notification.

Possible statuses are: error, warning, ok, exception.

Configuration:
```
monitor.<monitor-id>.notificator.<notificator-id>.type = statuses
monitor.<monitor-id>.notificator.<notificator-id>.statuses = <concerned statuses separated by space>
monitor.<monitor-id>.notificator.<notificator-id>.silent.period = <period like 1h, 3m or 45s> (default: 0)
```

Minimum period between two notifications is ".silent.period".

## Constant status notificator

If a metric has been in configured statuses during the configured period (and maximum times if configured), it produces a notification.

Possible statuses are: error, warning, ok, exception.

Configuration:
```
monitor.<monitor-id>.notificator.<notificator-id>.type = constant
monitor.<monitor-id>.notificator.<notificator-id>.statuses = <concerned statuses separated by space>
monitor.<monitor-id>.notificator.<notificator-id>.period = <period like 1h, 3m or 45s> (default: 15m)
monitor.<monitor-id>.notificator.<notificator-id>.max-times = <consicutive times>
monitor.<monitor-id>.notificator.<notificator-id>.silent.period = <period like 1h, 3m or 45s> (default: 0)
```

Minimum period between two notifications is ".period" plus ".silent.period".

An example of the result of this notificator can be seen in the following image.
![Constant status notificator](../img/notificator/constant.png)

## Percentage status notificator

If a metric has been in configured statuses during a percentage of the configured period, it produces a notification.

Possible statuses are: error, warning, ok, exception.

Configuration:
```
monitor.<monitor-id>.notificator.<notificator-id>.type = percentage
monitor.<monitor-id>.notificator.<notificator-id>.sinks = <ALL|notifications-sinks-ids> (default: ALL)
monitor.<monitor-id>.notificator.<notificator-id>.statuses = <concerned statuses separated by space>
monitor.<monitor-id>.notificator.<notificator-id>.period = <period like 1h, 3m or 45s> (default: 15m)
monitor.<monitor-id>.notificator.<notificator-id>.silent.period = <period like 1h, 3m or 45s> (default: 0)
monitor.<monitor-id>.notificator.<notificator-id>.percentage = <0-100> (default: 90)
```

Minimum period between two notifications is ".period" plus ".silent.period".

An example of the result of this notificator can be seen in the following image.
![Percentage status notificator](../img/notificator/percentage.png)