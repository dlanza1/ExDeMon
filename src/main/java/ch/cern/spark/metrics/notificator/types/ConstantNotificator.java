package ch.cern.spark.metrics.notificator.types;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ch.cern.TimeUtils;
import ch.cern.spark.Properties;
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
    private static Duration PERIOD_DEFAULT = Duration.ofMinutes(15);
    private Duration period = PERIOD_DEFAULT;
    
    private Instant constantlySeenFrom;
    
    public ConstantNotificator() {
        super(ConstantNotificator.class, "constant");
    }

    @Override
    public void config(Properties properties) throws Exception {
        super.config(properties);
        
        expectedStatuses = Stream.of(properties.getProperty(STATUSES_PARAM).split(","))
					        		.map(String::trim)
					        		.map(String::toUpperCase)
					        		.map(Status::valueOf)
					        		.collect(Collectors.toSet());

        period = properties.getPeriod(PERIOD_PARAM, PERIOD_DEFAULT);
    }
    
    @Override
    public void load(Store store) {
        if(store == null || !(store instanceof Store_))
            return;
        
        Store_ data = (Store_) store;
        
        if(data.constantlySeenFrom != null)
        		constantlySeenFrom = data.constantlySeenFrom.toInstant();
    }

    @Override
    public Store save() {
        Store_ store = new Store_();
        
        if(constantlySeenFrom != null)
        		store.constantlySeenFrom = Date.from(constantlySeenFrom);
        
        return store;
    }

    @Override
    public Optional<Notification> process(Status status, Instant timestamp) {
        boolean isExpectedStatus = isExpectedStatus(status);
        
        if(isExpectedStatus && constantlySeenFrom == null)
            constantlySeenFrom = timestamp;
        
        if(!isExpectedStatus)
            constantlySeenFrom = null;
        
        if(raise(timestamp)){
            Notification notification = new Notification();
            notification.setReason("Metric has been in state " 
                    + expectedStatuses + " for " + TimeUtils.toString(getDiff(timestamp))
                    + ".");
            
            constantlySeenFrom = null;
            
            return Optional.of(notification);
        }else{
            return Optional.empty();
        }
    }
    
    private boolean isExpectedStatus(Status status) {
        return expectedStatuses.contains(status);
    }
    
    private Duration getDiff(Instant timestamp){
        if(constantlySeenFrom == null)
            return Duration.ZERO;
        
        return Duration.between(constantlySeenFrom, timestamp).abs();
    }

    private boolean raise(Instant timestamp) {
        return getDiff(timestamp).compareTo(period) >= 0;
    }

    public static class Store_ implements Store{
        private static final long serialVersionUID = -1907347033980904180L;
        
        Date constantlySeenFrom;
    }

}
