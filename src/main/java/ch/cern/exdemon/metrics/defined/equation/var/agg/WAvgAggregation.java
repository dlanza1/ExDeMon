package ch.cern.exdemon.metrics.defined.equation.var.agg;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;

import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.metrics.DatedValue;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.utils.DurationAndTruncate;
import ch.cern.utils.Pair;

@RegisterComponentType("weighted_avg")
public class WAvgAggregation extends Aggregation {

    private static final long serialVersionUID = 8713765353223035040L;
    
    private DurationAndTruncate expire;
    
    public WAvgAggregation() {
    }
    
    @Override
    public Class<? extends Value> inputType() {
        return FloatValue.class;
    }

    @Override
    public Value aggregate(Collection<DatedValue> values, Instant time) {
        if(values.isEmpty())
            return new ExceptionValue("no values");
       
        Optional<Pair<Double, Double>> pairSum = values.stream().filter(value -> value.getValue().getAsFloat().isPresent())
                .map(datedValue -> {
                    double weight = computeWeight(time, datedValue.getTime());
                    
                    Value value = datedValue.getValue();
                    value = value.getAsAggregated().isPresent() ? value.getAsAggregated().get() : value;
                    
                    return new Pair<Double, Double>(weight, weight * value.getAsFloat().get());
                })
                .reduce((p1, p2) -> new Pair<Double, Double>(p1.first + p2.first, p1.second + p2.second));

        if(!pairSum.isPresent())
            return new ExceptionValue("no float values");
        
        double totalWeights = pairSum.get().first;
        double weightedValues = pairSum.get().second;
        
        return new FloatValue(weightedValues / totalWeights);
    }
    
    private float computeWeight(Instant time, Instant metric_timestamp) {
        Duration time_difference = Duration.between(time, metric_timestamp).abs();
        
        if(expire.getDuration().compareTo(time_difference) < 0)
                return 0;
                        
        return (float) (expire.getDuration().getSeconds() - time_difference.getSeconds()) / (float) expire.getDuration().getSeconds();
    }
    
    @Override
    public Class<? extends Value> returnType() {
        return FloatValue.class;
    }
    
    public void setExpire(DurationAndTruncate expire) {
        this.expire = expire;
    }

}
