package ch.cern.spark.metrics.defined.equation.var.agg;

import java.time.Instant;
import java.util.Collection;

import ch.cern.components.RegisterComponent;
import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.ValueHistory;
import ch.cern.spark.metrics.defined.equation.var.MetricVariable;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;

@RegisterComponent("successive_count")
public class SuccessiveCountAgregation extends Aggregation {

    private static final long serialVersionUID = 8274088208286521725L;

    @Override
    public Class<? extends Value> inputType() {
        return Value.class;
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
    
    @Override
    public boolean isFilterEnable() {
        return false;
    }

    @Override
    public void postUpdateStatus(MetricVariable metricVariable, AggregationValues aggValues, Metric metric) {
        if(!metricVariable.getFilter().test(metric))
            aggValues.reset();
    }
    
    @Override
    public void postUpdateStatus(MetricVariable metricVariable, ValueHistory history, Metric metric) {
        if(!metricVariable.getFilter().test(metric))
            history.reset();
    }
    
}
