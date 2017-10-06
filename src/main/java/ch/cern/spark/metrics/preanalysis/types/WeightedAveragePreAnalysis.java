package ch.cern.spark.metrics.preanalysis.types;

import java.util.Date;

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
    public static final long PERIOD_DEFAULT = 5 * 60;

    private ValueHistory history;

    private long period_in_seconds;
    
    public WeightedAveragePreAnalysis() {
        super(WeightedAveragePreAnalysis.class, "weighted-average");
    }

    @Override
    public void config(Properties properties) throws Exception {
        super.config(properties);
        
        period_in_seconds = getPeriod(properties);
        
        history = new ValueHistory(period_in_seconds);
    }

    private long getPeriod(Properties properties) {
        String period_value = properties.getProperty(PERIOD_PARAM);
        if(period_value != null)
            return StringUtils.parseStringWithTimeUnitToSeconds(period_value);
        
        return PERIOD_DEFAULT;
    }

    @Override
    public void load(Store store) {
        history = ((ValueHistory.Store_) store).history;
        
        history.setPeriod(period_in_seconds);
    }
    
    @Override
    public Store save() {
        ValueHistory.Store_ store = new ValueHistory.Store_();
        
        store.history = history;
        
        return store;
    }
    
    @Override
    public float process(Date metric_timestamp, float metric_value) {
        history.add(metric_timestamp, metric_value);
        
        Float newValue = getAvergaeForTime(metric_timestamp);
        
        return newValue != null ? newValue : metric_value;
    }
    
    public Float getAvergaeForTime(Date time){
        history.removeRecordsOutOfPeriodForTime(time);
        
        return computeAverageForTime(time);
    }

    private Float computeAverageForTime(Date time) {
        if(history.size() == 0)
            return null;
        
        float total_weight = 0;
        float acummulator = 0;
        
        float first_value = Float.NaN;
        boolean all_same = true;
        
        for (DatedValue value : history.getDatedValues()){
            boolean metricTimeIsOlder = value.getDate().compareTo(time) > 0; 
            
            if(metricTimeIsOlder){
                return acummulator / total_weight;
            }else{                
                float weight = computeWeight(time, value.getDate());
                float value_float = value.getValue();
                
                total_weight += weight;
                acummulator += value_float * weight;
                
                if(Float.isNaN(first_value))
                    first_value = value_float;
                else if(first_value != value_float)
                    all_same = false;
            }
        }
        
        if(total_weight == 0)
            return null;
        
        if(all_same)
            return first_value;
        
        return acummulator / total_weight;
    }

    private float computeWeight(Date time, Date metric_time) {
        long time_difference_in_seconds = computeTimeDifferenceInSeconds(time, metric_time);
        
        if(time_difference_in_seconds >= period_in_seconds)
            return 0;
        else
            return (float) (period_in_seconds - time_difference_in_seconds) / (float) period_in_seconds;
    }

    private long computeTimeDifferenceInSeconds(Date time1, Date time2) {
        long time1_in_seconds = time1.getTime() / 1000;
        long time2_in_seconds = time2.getTime() / 1000;
        
        return Math.abs(time1_in_seconds - time2_in_seconds);
    }

    public void reset() {
        if(history != null)
            history.reset();
    }

}
