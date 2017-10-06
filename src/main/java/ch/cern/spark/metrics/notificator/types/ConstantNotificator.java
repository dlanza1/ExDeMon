package ch.cern.spark.metrics.notificator.types;

import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import ch.cern.spark.Properties;
import ch.cern.spark.StringUtils;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notificator.Notificator;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.store.Store;

public class ConstantNotificator extends Notificator implements HasStore {
    
    private static final long serialVersionUID = -7890231998987060652L;
    
    private String STATUSES_PARAM = "statuses";
    private Set<Status> expectedStatuses;
    
    private static String PERIOD_PARAM = "period";
    private static String PERIOD_DEFAULT = "15m";
    private long period;
    
    private Date constantlySeenFrom;
    
    public ConstantNotificator() {
        super(ConstantNotificator.class, "constant");
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
    }
    
    @Override
    public void load(Store store) {
        if(store == null || !(store instanceof Store_))
            return;
        
        Store_ data = (Store_) store;
        
        constantlySeenFrom = data.constantlySeenFrom;
    }

    @Override
    public Store save() {
        Store_ store = new Store_();
        
        store.constantlySeenFrom = constantlySeenFrom;
        
        return store;
    }

    @Override
    public Notification process(Status status, Date timestamp) {
        boolean isExpectedStatus = isExpectedStatus(status);
        
        if(isExpectedStatus && constantlySeenFrom == null)
            constantlySeenFrom = timestamp;
        
        if(!isExpectedStatus)
            constantlySeenFrom = null;
        
        if(raise(timestamp)){
            Notification notification = new Notification();
            notification.setReason("Metric has been in state " 
                    + expectedStatuses + " for " + StringUtils.secondsToString(getDiff(timestamp))
                    + ".");
            
            constantlySeenFrom = null;
            
            return notification;
        }else{
            return null;
        }
    }
    
    private boolean isExpectedStatus(Status status) {
        return expectedStatuses.contains(status);
    }
    
    private long getDiff(Date timestamp){
        if(constantlySeenFrom == null)
            return 0;
        
        return timestamp.getTime() / 1000 - constantlySeenFrom.getTime() / 1000;
    }

    private boolean raise(Date timestamp) {
        return getDiff(timestamp) >= period;
    }

    public static class Store_ implements Store{
        private static final long serialVersionUID = -1907347033980904180L;
        
        Date constantlySeenFrom;
    }

}
