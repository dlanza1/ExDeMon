package ch.cern.spark.metrics.preanalysis.types;

import java.time.Duration;
import java.time.Instant;
import java.util.OptionalDouble;

import ch.cern.spark.Properties;
import ch.cern.spark.metrics.ValueHistory;
import ch.cern.spark.metrics.preanalysis.PreAnalysis;
import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.store.Store;

public class AveragePreAnalysis extends PreAnalysis implements HasStore{
    
    private static final long serialVersionUID = -8910030746737888613L;

    public static final String PERIOD_PARAM = "period";
    public static final Duration PERIOD_DEFAULT = Duration.ofMinutes(5);
    private Duration period;
    
    private ValueHistory history;
    
    public AveragePreAnalysis() {
        super(AveragePreAnalysis.class, "average");
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
    public float process(Instant metric_timestamp, float metric_value) {
        history.add(metric_timestamp, metric_value);
        history.purge(metric_timestamp);
        
        OptionalDouble newValue = history.getDatedValues().stream()
											.mapToDouble(value -> value.getValue())
											.average();
        
        return (float) newValue.orElse(metric_value);
    }

    public void reset() {
        if(history != null)
            history.reset();
    }

}
