package ch.cern.spark.metrics.notificator.types;

import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import ch.cern.spark.Pair;
import ch.cern.spark.Properties;
import ch.cern.spark.StringUtils;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notificator.Notificator;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.store.Store;

public class PercentageNotificator extends Notificator implements HasStore {
    
    private static final long serialVersionUID = -7890231998987060652L;
    
    private String STATUSES_PARAM = "statuses";
    private Set<Status> expectedStatuses;
    
    private static String PERIOD_PARAM = "period";
    private static String PERIOD_DEFAULT = "15m";
    private long period;
    
    private String PERCENTAGE_PARAM = "percentage";
    private String PERCENTAGE_DEFAULT = "90";
    private float percentage;
    
    private List<Pair<Date, Boolean>> hits;
    
    public PercentageNotificator() {
        super(PercentageNotificator.class, "percentage");
        
        hits = new LinkedList<>();
    }

    @Override
    public void config(Properties properties) throws Exception {
        super.config(properties);
        
        expectedStatuses = new HashSet<>();
        String[] statusesFromConf = properties.getProperty(STATUSES_PARAM).toUpperCase().split(",");
        for (String statusFromConf : statusesFromConf) {
            expectedStatuses.add(Status.valueOf(statusFromConf.trim().toUpperCase()));
        }
        
        period = StringUtils.parseStringWithTimeUnitToSeconds(properties.getProperty(PERIOD_PARAM, PERIOD_DEFAULT));
        
        String percentage_s = properties.getProperty(PERCENTAGE_PARAM, PERCENTAGE_DEFAULT);
        percentage = Float.valueOf(percentage_s);
    }
    
    @Override
    public void load(Store store) {
        if(store == null || !(store instanceof Store_))
            return;
        
        Store_ data = (Store_) store;
        
        hits = data.hits;
    }

    @Override
    public Store save() {
        Store_ store = new Store_();
        
        store.hits = hits;
        
        return store;
    }

    @Override
    public Notification process(Status status, Date timestamp) {
        removeExpiredHits(timestamp);
        
        hits.add(new Pair<Date, Boolean>(timestamp, isExpectedStatus(status)));
        
        long coveredPeriod = getCoveredPeriod(timestamp);
        if(coveredPeriod < period)
            return null;
        
        if(raise(timestamp)){
            Notification notification = new Notification();
            notification.setReason("Metric has been " + percentage + "% of the last "
                    + StringUtils.secondsToString(period)
                    + " in state " + expectedStatuses + ".");
            
            hits = new LinkedList<>();
            
            return notification;
        }else{
            return null;
        }
    }

    private long getCoveredPeriod(Date timestamp) {
        Date oldestTimestamp = getOldestTimestamp();
        
        if(oldestTimestamp == null)
            return 0;
        
        return (timestamp.getTime() - oldestTimestamp.getTime()) / 1000;
    }

    private Date getOldestTimestamp() {
        Date oldestTimestamp = null;
        
        for (Pair<Date, Boolean> hit : hits)
            if(oldestTimestamp == null || oldestTimestamp.getTime() > hit.first.getTime())
                oldestTimestamp = hit.first;
        
        return oldestTimestamp;
    }

    private void removeExpiredHits(Date time) {
        // It needs to store a longer period than the configured period
        // Because percentage is calculated for, as minimum, the period
        long extendedPeriod = (long) (period * 1.2f);
        
        long expirationTime = (time.getTime()/1000 - extendedPeriod) * 1000;
        
        Iterator<Pair<Date, Boolean>> it = hits.iterator();
        while(it.hasNext())
            if(it.next().first.getTime() < expirationTime)
                it.remove();
    }

    private boolean isExpectedStatus(Status status) {
        return expectedStatuses.contains(status);
    }

    private boolean raise(Date currentTime) {
        float percentageOfHits = computePercentageOfHits(currentTime);
        
        return percentageOfHits > (percentage / 100f);
    }

    private float computePercentageOfHits(Date currentTime) {
        float counter_true = 0;
        float counter = 0;
        
        for (Pair<Date, Boolean> pair : hits)
            if(isInPeriod(currentTime, pair.first)){
                counter++;
                
                if(pair.second)
                    counter_true++;
            }
                
        
        return counter_true / counter;
    }

    private boolean isInPeriod(Date currentTime, Date hitTime) {
        long newestTime = currentTime.getTime();
        long oldestTime = newestTime - period * 1000;
        
        long time = hitTime.getTime();
        
        return time >= oldestTime && time <= newestTime;
    }

    public static class Store_ implements Store{
        private static final long serialVersionUID = -1907347033980904180L;
        
        List<Pair<Date, Boolean>> hits;
    }

}
