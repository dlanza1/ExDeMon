package ch.cern.spark.metrics.preanalysis.types;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;

import ch.cern.spark.Pair;
import ch.cern.spark.Properties;
import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.ValueHistory;
import ch.cern.spark.metrics.preanalysis.PreAnalysis;
import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.store.Store;

public class WeightedAveragePreAnalysis extends PreAnalysis implements HasStore{
    
    private static final long serialVersionUID = -8910030746737888613L;
    
    public static final String PERIOD_PARAM = "period";
    public static final Duration PERIOD_DEFAULT = Duration.ofMinutes(5);
    private Duration period;
    
    private ValueHistory history;
    
    public WeightedAveragePreAnalysis() {
        super(WeightedAveragePreAnalysis.class, "weighted-average");
    }

    @Override
    public void config(Properties properties) throws Exception {
        super.config(properties);
        
        period = properties.getPeriod(PERIOD_PARAM, PERIOD_DEFAULT).get();
        
        history = new ValueHistory(period);
    }

    @Override
    public void load(Store store) {
        history = ((ValueHistory.Store_) store).history;
        
        history.setPeriod(period);
    }
    
    @Override
    public Store save() {
        ValueHistory.Store_ store = new ValueHistory.Store_();
        
        store.history = history;
        
        return store;
    }
    
    @Override
    public double process(Instant metric_timestamp, double metric_value) {
        history.add(metric_timestamp, (float) metric_value);
        history.purge(metric_timestamp);
        
        OptionalDouble newValue = computeAverageForTime(metric_timestamp);
        
        return newValue.orElse(metric_value);
    }

    private OptionalDouble computeAverageForTime(Instant time) {
        List<DatedValue> values = history.getDatedValues();

        Optional<Pair<Double, Double>> pairSum = values.stream()
        									.map(value -> {
        										double weight = computeWeight(time, value.getInstant());
										    	
									    		return new Pair<Double, Double>(weight, weight * value.getValue());
        									})
        									.reduce((p1, p2) -> new Pair<Double, Double>(p1.first + p2.first, p1.second + p2.second));
        
        if(!pairSum.isPresent())
            return OptionalDouble.empty();
        
        double totalWeights = pairSum.get().first;
        double weightedValues = pairSum.get().second;
        
        return OptionalDouble.of(weightedValues / totalWeights);
    }

    private float computeWeight(Instant time, Instant metric_timestamp) {
        Duration time_difference = Duration.between(time, metric_timestamp).abs();
        
        if(period.compareTo(time_difference) < 0)
        		return 0;
        				
        return (float) (period.getSeconds() - time_difference.getSeconds()) / (float) period.getSeconds();
    }

    public void reset() {
        if(history != null)
            history.reset();
    }

}
