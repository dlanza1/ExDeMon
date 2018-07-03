package ch.cern.exdemon.metrics.defined.equation.var.agg;

import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.metrics.DatedValue;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.Value;

@RegisterComponentType("diff")
public class DiffAggregation extends Aggregation {

    private static final long serialVersionUID = 8713765353223035040L;
    
    public DiffAggregation() {
    }
    
    @Override
    public Class<? extends Value> inputType() {
        return FloatValue.class;
    }

    @Override
    public Value aggregate(Collection<DatedValue> values, Instant time) {
        if(values.size() <= 1)
            return new ExceptionValue("diff requires at least two values");
        
        List<DatedValue> sorted = values.stream()
                .sorted()
                .collect(Collectors.toList());

        Value lastValue = sorted.get(sorted.size() - 1).getValue();
        Value previousValue = sorted.get(sorted.size() - 2).getValue();
        
        if(previousValue.getAsAggregated().isPresent() || lastValue.getAsAggregated().isPresent())
            return new ExceptionValue("latest values have been summarized, diff cannot be computed.");
        
        double lastValueD = lastValue.getAsFloat().get();
        double previousValueD = previousValue.getAsFloat().get();
        
        return new FloatValue(lastValueD - previousValueD);
    }
    
    @Override
    public Class<? extends Value> returnType() {
        return FloatValue.class;
    }

}
