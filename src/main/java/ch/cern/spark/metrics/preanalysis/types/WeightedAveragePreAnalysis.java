package ch.cern.spark.metrics.preanalysis.types;

import java.time.Duration;
import java.time.Instant;

import ch.cern.spark.Properties;
import ch.cern.spark.StringUtils;
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
        
        period = getPeriod(properties);
        
        history = new ValueHistory(period);
    }

    private Duration getPeriod(Properties properties) {
        String period_value = properties.getProperty(PERIOD_PARAM);
        if(period_value != null)
            return Duration.ofSeconds(StringUtils.parseStringWithTimeUnitToSeconds(period_value));
        
        return PERIOD_DEFAULT;
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
        
        Float newValue = getAvergaeForTime(metric_timestamp);
        
        return newValue != null ? newValue : metric_value;
    }
    
    public Float getAvergaeForTime(Instant metric_timestamp){
        history.removeRecordsOutOfPeriodForTime(metric_timestamp);
        
        return computeAverageForTime(metric_timestamp);
    }

    private Float computeAverageForTime(Instant metric_timestamp) {
        if(history.size() == 0)
            return null;
        
        float total_weight = 0;
        float acummulator = 0;
        
        float first_value = Float.NaN;
        boolean all_same = true;
        
        for (DatedValue value : history.getDatedValues()){              
            float weight = computeWeight(metric_timestamp, value.getInstant());
            float value_float = value.getValue();
            
            total_weight += weight;
            acummulator += value_float * weight;
            
            if(Float.isNaN(first_value))
                first_value = value_float;
            else if(first_value != value_float)
                all_same = false;
        }
        
        if(total_weight == 0)
            return null;
        
        if(all_same)
            return first_value;
        
        return acummulator / total_weight;
    }

    private float computeWeight(Instant time, Instant metric_timestamp) {
        Duration time_difference = Duration.between(time, metric_timestamp).abs();
        
        if(time_difference.compareTo(period) > 0)
            return 0;
        else
            return (float) (period.getSeconds() - time_difference.getSeconds()) / (float) period.getSeconds();
    }

    public void reset() {
        if(history != null)
            history.reset();
    }

}
