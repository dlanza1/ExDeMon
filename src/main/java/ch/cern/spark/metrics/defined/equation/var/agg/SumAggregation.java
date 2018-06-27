package ch.cern.spark.metrics.defined.equation.var.agg;

import java.time.Instant;
import java.util.Collection;

import ch.cern.components.RegisterComponentType;
import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;

@RegisterComponentType("sum")
public class SumAggregation extends Aggregation {

    private static final long serialVersionUID = 8713765353223035040L;
    
    public SumAggregation() {
    }
    
    @Override
    public Class<? extends Value> inputType() {
        return FloatValue.class;
    }

    @Override
    public Value aggregate(Collection<DatedValue> values, Instant time) {
        if(values.isEmpty())
            return new ExceptionValue("no values");
        
        return new FloatValue(values.stream()
                                        .map(v -> v.getValue().getAsAggregated().isPresent() ?
                                                    v.getValue().getAsAggregated().get().getAsFloat().get() :
                                                    v.getValue().getAsFloat().get())
                                        .mapToDouble(d -> d)
                                        .sum());
    }
    
    @Override
    public Class<? extends Value> returnType() {
        return FloatValue.class;
    }

}
