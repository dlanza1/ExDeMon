package ch.cern.spark.metrics.defined.equation.var.agg;

import java.time.Instant;
import java.util.Collection;
import java.util.stream.Collectors;

import ch.cern.components.RegisterComponentType;
import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.Value;

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

}
