package ch.cern.spark.metrics.preanalysis.types;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.OptionalDouble;

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
        
        period = properties.getPeriod(PERIOD_PARAM, PERIOD_DEFAULT);
        
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
    public float process(Instant metric_timestamp, float metric_value) {
        history.add(metric_timestamp, metric_value);
        history.purge(metric_timestamp);
        
        OptionalDouble newValue = computeAverageForTime(metric_timestamp);
        
        return (float) newValue.orElse(metric_value);
    }

    private OptionalDouble computeAverageForTime(Instant time) {
        List<DatedValue> values = history.getDatedValues();

        double total_weight = values.stream().mapToDouble(value -> computeWeight(time, value.getInstant())).sum();
        double acummulator = values.stream().mapToDouble(value -> computeWeight(time, value.getInstant()) * value.getValue() ).sum();
        
        if(total_weight == 0)
            return OptionalDouble.empty();
        
        return OptionalDouble.of(acummulator / total_weight);
    }

    private double computeWeight(Instant time, Instant metric_timestamp) {
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
