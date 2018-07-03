package ch.cern.exdemon.metrics.defined.equation.var.agg;

import java.time.Instant;
import java.util.Collection;

import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.metrics.DatedValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.Value;

@RegisterComponentType("count_floats")
public class CountFloatsAgregation extends Aggregation {

    private static final long serialVersionUID = 8274088208286521725L;

    @Override
    public Class<? extends Value> inputType() {
        return FloatValue.class;
    }

    @Override
    public Value aggregate(Collection<DatedValue> values, Instant time) {
        return new FloatValue(values.stream()
                                        .mapToDouble(v -> v.getValue().getAsAggregated().isPresent() ? 
                                                                v.getValue().getAsAggregated().get().getAsFloat().get() : 
                                                                1)
                                        .sum());
    }

    @Override
    public Class<? extends Value> returnType() {
        return FloatValue.class;
    }

}
