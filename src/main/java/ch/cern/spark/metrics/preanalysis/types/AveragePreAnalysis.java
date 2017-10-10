package ch.cern.spark.metrics.preanalysis.types;

import java.time.Duration;
import java.time.Instant;

import ch.cern.spark.Properties;
import ch.cern.spark.metrics.DatedValue;
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
        
        Float newValue = getAvergaeForTime(metric_timestamp);
        
        return newValue != null ? newValue : metric_value;
    }
    
    public Float getAvergaeForTime(Instant metric_timestamp){
        history.removeRecordsOutOfPeriodForTime(metric_timestamp);
        
        return computeAverage();
    }

    private Float computeAverage() {
        if(history.size() == 0)
            return null;
        
        float acummulator = 0;
        int count = 0;
        
        for (DatedValue value : history.getDatedValues()){
            acummulator += value.getValue();
            count++;
        }
        
        if(count == 0)
            return null;
        
        return acummulator / count;
    }

    public void reset() {
        if(history != null)
            history.reset();
    }

}
