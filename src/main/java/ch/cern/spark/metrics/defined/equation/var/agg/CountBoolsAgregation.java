package ch.cern.spark.metrics.defined.equation.var.agg;

import java.time.Instant;
import java.util.Collection;

import ch.cern.components.RegisterComponent;
import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;

@RegisterComponent("count_bools")
public class CountBoolsAgregation extends Aggregation {

    private static final long serialVersionUID = 8274088208286521725L;

    @Override
    public Class<? extends Value> inputType() {
        return BooleanValue.class;
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
