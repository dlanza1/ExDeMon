package ch.cern.spark.metrics.preanalysis.types;

import java.util.Date;

import ch.cern.spark.Properties;
import ch.cern.spark.StringUtils;
import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.ValueHistory;
import ch.cern.spark.metrics.preanalysis.PreAnalysis;
import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.store.Store;

public class AveragePreAnalysis extends PreAnalysis implements HasStore{
    
    private static final long serialVersionUID = -8910030746737888613L;

    public static final String PERIOD_PARAM = "period";
    public static final long PERIOD_DEFAULT = 5 * 60;
    private long period_in_seconds;
    
    private ValueHistory history;
    
    public AveragePreAnalysis() {
        super(AveragePreAnalysis.class, "average");
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
        
        float acummulator = 0;
        int count = 0;
        
        for (DatedValue value : history.getDatedValues()){
            boolean metricTimeIsOlder = value.getDate().compareTo(time) > 0; 
            
            if(metricTimeIsOlder){
                return acummulator / count;
            }else{
                acummulator += value.getValue();
                count++;
            }
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
