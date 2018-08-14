package ch.cern.exdemon.metrics.defined.equation.var.agg;

import java.time.Instant;
import java.util.Collection;
import java.util.stream.Collectors;

import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.metrics.DatedValue;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.ValueHistory;
import ch.cern.exdemon.metrics.defined.equation.var.ValueVariable;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.exdemon.metrics.value.Value;

@RegisterComponentType("last")
public class LastValueAggregation extends Aggregation {

    private static final long serialVersionUID = -2652766061746715818L;
    
    private String typeClassName;

    public LastValueAggregation(Class<? extends Value> type) {
        this.typeClassName = type.getName();
    }

    @Override
    public Class<? extends Value> inputType() {
        return Value.class;
    }

    @Override
    public Value aggregate(Collection<DatedValue> values, Instant time) {
        if(values.isEmpty())
            return new ExceptionValue("no values");
        
        Value value = values.stream().sorted().collect(Collectors.toList()).get(values.size() - 1).getValue();
        
        return value.getAsAggregated().isPresent() ? value.getAsAggregated().get() : value;
    }

    @Override
    public Class<? extends Value> returnType() {
        return getClassType();
    }

    @SuppressWarnings("unchecked")
    private Class<? extends Value> getClassType() {
        try {
            return (Class<? extends Value>) Class.forName(typeClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Class not found");
        }
    }
    
    @Override
    public void postUpdateStatus(ValueVariable metricVariable, ValueHistory history, Metric metric) {
        history.reset();
        history.add(metric.getTimestamp(), metric.getValue(), metric);
    }

}
