package ch.cern.exdemon.metrics.defined.equation.var;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentBuildResult;
import ch.cern.exdemon.components.ComponentTypes;
import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.metrics.DatedValue;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.ValueHistory;
import ch.cern.exdemon.metrics.defined.equation.ComputationException;
import ch.cern.exdemon.metrics.defined.equation.var.agg.Aggregation;
import ch.cern.exdemon.metrics.defined.equation.var.agg.AggregationValues;
import ch.cern.exdemon.metrics.defined.equation.var.agg.LastValueAggregation;
import ch.cern.exdemon.metrics.defined.equation.var.agg.WAvgAggregation;
import ch.cern.exdemon.metrics.filter.MetricsFilter;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusValue;
import ch.cern.utils.DurationAndTruncate;
import ch.cern.utils.TimeUtils;
import lombok.Getter;

public class ValueVariable extends Variable {

    public static final long MAX_SIZE_DEFAULT = 10000;

    @Getter
    private MetricsFilter filter;

    @Getter
    private Aggregation aggregation;

    protected DurationAndTruncate expire;
    protected DurationAndTruncate ignore;

    private Set<String> aggregateSelectAtt;
    private boolean aggregateSelectALL = false;

    private int max_aggregation_size;
    private int max_lastAggregatedMetrics_size;

    private ChronoUnit granularity;

    public ValueVariable(String name) {
        super(name);
    }

    public ConfigurationResult config(Properties properties, Optional<Class<? extends Value>> typeOpt) {
        ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
        
        try {
            filter = MetricsFilter.build(properties.getSubset("filter"));
        } catch (ConfigurationException e) {
            confResult.withError("filter", e);
        }

        if (properties.containsKey("expire") && properties.getProperty("expire").toLowerCase().equals("never"))
            expire = null;
        else
            try {
                expire = DurationAndTruncate.from(properties.getProperty("expire", "10m"));
            } catch (ConfigurationException e) {
                confResult.withError("expire", e);
            }

        String aggregateVal = properties.getProperty("aggregate.type");
        if (aggregateVal != null) {
            ComponentBuildResult<Aggregation> aggregationBuildResult = ComponentTypes.build(Type.AGGREGATION, properties.getSubset("aggregate"));
            confResult.merge("aggregate", aggregationBuildResult.getConfigurationResult());
            
            if(aggregationBuildResult.getComponent().isPresent()) {
                aggregation = aggregationBuildResult.getComponent().get();

                if (aggregation instanceof WAvgAggregation)
                    ((WAvgAggregation) aggregation).setExpire(expire);

                if (typeOpt.isPresent() && !aggregation.returnType().equals(typeOpt.get()))
                    confResult.withError(name, "variable " + name + " returns type "
                            + aggregation.returnType().getSimpleName() + " because of its aggregation operation, "
                            + "but in the equation there is a function that uses it as type "
                            + typeOpt.get().getSimpleName());
            }
        } else {
            if (typeOpt.isPresent())
                aggregation = new LastValueAggregation(typeOpt.get());
            else
                aggregation = new LastValueAggregation(Value.class);
        }

        if (!properties.containsKey("ignore"))
            ignore = null;
        else
            try {
                ignore = DurationAndTruncate.from(properties.getProperty("ignore"));
            } catch (ConfigurationException e) {
                confResult.withError("ignore", e);
            }

        String granularityString = properties.getProperty("aggregate.history.granularity");
        if (granularityString != null)
            try {
                granularity = TimeUtils.parseGranularity(granularityString);
            } catch (ConfigurationException e) {
                confResult.withError("aggregate.history.granularity", e);
            }

        String aggregateSelect = properties.getProperty("aggregate.attributes");
        if (aggregateSelect != null && aggregateSelect.equals("ALL"))
            aggregateSelectALL = true;
        else if (aggregateSelect != null)
            aggregateSelectAtt = new HashSet<String>(Arrays.asList(aggregateSelect.split("\\s")));

        max_aggregation_size = (int) properties.getFloat("aggregate.max-size", MAX_SIZE_DEFAULT);

        max_lastAggregatedMetrics_size = (int) properties.getFloat("aggregate.latest-metrics.max-size", 0);
        if(max_lastAggregatedMetrics_size > 100) {
            confResult.withWarning("aggregate.latest-metrics.max-size", "can be maximun 100, new value = 100");
            
            max_lastAggregatedMetrics_size = 100;
        }

        return confResult;
    }

    @Override
    public boolean test(Metric metric) {
        if(!aggregation.isFilterEnable())
            return true;
        
        return filter.test(metric);
    }

    @Override
    public Value compute(VariableStatuses variableStatuses, Instant time) {
        StatusValue status = variableStatuses.get(name);

        Value aggValue = null;
        try {
            Collection<DatedValue> values = getDatedValues(status, time, aggregation.inputType());

            aggValue = aggregation.aggregateValues(values, time);

            if (aggValue.getAsAggregated().isPresent())
                aggValue = aggValue.getAsAggregated().get();

            aggValue.setLastSourceMetrics(getLastAggregatedMetrics(status));
        } catch (ComputationException e) {
            aggValue = new ExceptionValue(e.getMessage());
        }

        String source = aggValue.toString();
        if (aggValue.getAsException().isPresent())
            aggValue = new ExceptionValue("Variable " + name + ": " + aggValue.getAsException().get());

        String aggName = aggregation.getClass().getAnnotation(RegisterComponentType.class).value();
        aggValue.setSource(aggName.toLowerCase() + "(var(" + name + "))=" + source);

        return aggValue;
    }

    private List<Metric> getLastAggregatedMetrics(StatusValue status) {
        if (status instanceof AggregationValues) {
            AggregationValues aggValues = (AggregationValues) status;

            Map<Integer, Metric> metrics = aggValues.getLastAggregatedMetrics();

            return metrics != null ? new LinkedList<>(metrics.values()) : null;
        } else if (status instanceof ValueHistory.Status) {
            ValueHistory history = ((ValueHistory.Status) status).history;

            List<Metric> metrics = history.getLastAggregatedMetrics();

            return metrics != null ? new LinkedList<>(metrics) : null;
        }

        return null;
    }

    private Collection<DatedValue> getDatedValues(StatusValue status, Instant time, Class<? extends Value> inputType)
            throws ComputationException {
        Collection<DatedValue> values = new LinkedList<>();

        if (status == null)
            return values;

        if (isThereSelectedAttributes() && status instanceof AggregationValues) {
            AggregationValues aggValues = ((AggregationValues) status);

            if (expire != null)
                aggValues.purge(expire.adjustMinus(time));

            values = aggValues.getDatedValues();
        } else if (!isThereSelectedAttributes() && status instanceof ValueHistory.Status) {
            ValueHistory history = ((ValueHistory.Status) status).history;

            if (expire != null)
                history.purge(expire.adjustMinus(time));

            values = history.getDatedValues();
        }

        if (ignore != null) {
            Instant latestTime = ignore.adjustMinus(time);

            values = values.stream().filter(val -> val.getTime().isBefore(latestTime)).collect(Collectors.toList());
        }

        return values;
    }

    @Override
    public StatusValue updateStatus(Optional<StatusValue> statusOpt, Metric metric, Metric originalMetric) {
        StatusValue status = statusOpt.isPresent() ? statusOpt.get() : initStatus();
        
        metric.setAttributes(getAggSelectAttributes(metric.getAttributes()));

        if (isThereSelectedAttributes()) {
            if (!(status instanceof AggregationValues))
                status = initStatus();

            AggregationValues aggValues = ((AggregationValues) status);

            aggValues.setMax_aggregation_size(max_aggregation_size);
            aggValues.setMax_lastAggregatedMetrics_size(max_lastAggregatedMetrics_size);

            int hash = 0;

            if (aggregateSelectALL)
                hash = metric.getAttributes().hashCode();

            if (aggregateSelectAtt != null)
                hash = metric.getAttributes().entrySet().stream()
                                                .filter(e -> aggregateSelectAtt.contains(e.getKey()))
                                                .collect(Collectors.toList()).hashCode();

            if (aggregateSelectALL || (hash != 1 && hash != 0))
                aggValues.add(hash, metric.getValue(), metric.getTimestamp(), metric, originalMetric);
            
            aggregation.postUpdateStatus(this, aggValues, metric);
        } else {
            if (!(status instanceof ValueHistory.Status))
                status = initStatus();

            ValueHistory history = ((ValueHistory.Status) status).history;

            history.setGranularity(granularity);
            history.setAggregation(aggregation);
            history.setMax_size(max_aggregation_size);
            history.setMax_lastAggregatedMetrics_size(max_lastAggregatedMetrics_size);
            history.add(metric.getTimestamp(), metric.getValue(), originalMetric);
            
            aggregation.postUpdateStatus(this, history, metric);
        }

        return status;
    }

    private boolean isThereSelectedAttributes() {
        return aggregateSelectAtt != null || aggregateSelectALL;
    }

    private StatusValue initStatus() {
        if (isThereSelectedAttributes())
            return new AggregationValues(max_aggregation_size, max_lastAggregatedMetrics_size);
        else
            return new ValueHistory.Status(max_aggregation_size, max_lastAggregatedMetrics_size, granularity, aggregation);
    }

    private Map<String, String> getAggSelectAttributes(Map<String, String> attributes) {
        if (aggregateSelectAtt == null || aggregateSelectALL)
            return attributes;

        return attributes.entrySet().stream().filter(entry -> aggregateSelectAtt.contains(entry.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    @Override
    public Class<? extends Value> returnType() {
        return aggregation.returnType();
    }

    @Override
    public String toString() {
        String aggName = aggregation.getClass().getAnnotation(RegisterComponentType.class).value();
        return aggName + "(time_filter(" + name + ", from:" + expire + ", to:" + ignore + "))";
    }

}
