package ch.cern.exdemon.monitor.trigger.types;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;
import ch.cern.exdemon.monitor.trigger.Trigger;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.HasStatus;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;
import ch.cern.utils.TimeUtils;
import lombok.ToString;

@ToString
@RegisterComponentType("constant")
public class ConstantTrigger extends Trigger implements HasStatus {
    
    private static final long serialVersionUID = -7890231998987060652L;

    private static final String STATUSES_PARAM = "statuses";
    private Set<Status> expectedStatuses;
    
    private static final String PERIOD_PARAM = "period";
    private static Duration PERIOD_DEFAULT = Duration.ofMinutes(15);
    private Duration period = PERIOD_DEFAULT;
    
    private Instant constantlySeenFrom;
    
    private static final String MAX_TIMES_PARAM = "max-times";
    private Integer maxTimes = null;
    private int times = 0;

    @Override
    public ConfigurationResult config(Properties properties) {
        ConfigurationResult configResult = super.config(properties);
        
        expectedStatuses = Stream.of(properties.getProperty(STATUSES_PARAM).split("\\s"))
					        		.map(String::trim)
					        		.map(String::toUpperCase)
					        		.map(Status::valueOf)
					        		.collect(Collectors.toSet());

        try {
            period = properties.getPeriod(PERIOD_PARAM, PERIOD_DEFAULT);
        } catch (ConfigurationException e) {
            configResult.withError(null, e);
        }
        
        String maxTimesVal = properties.getProperty(MAX_TIMES_PARAM);
        maxTimes = maxTimesVal != null ? Integer.parseInt(maxTimesVal) : null;
        
        return configResult.merge(null, properties.warningsIfNotAllPropertiesUsed());
    }
    
    @Override
    public void load(StatusValue store) {
        if(store == null || !(store instanceof Status_))
            return;
        
        Status_ data = (Status_) store;
        
        constantlySeenFrom = data.constantlySeenFrom;
        times = data.times;
    }

    @Override
    public StatusValue save() {
        Status_ store = new Status_();
        
        store.constantlySeenFrom = constantlySeenFrom;
        store.times = times;
        
        return store;
    }

    @Override
    public Optional<String> process(Status status, Instant timestamp) {
        boolean isExpectedStatus = isExpectedStatus(status);
        
        if(isExpectedStatus && maxTimes != null)
            times++;
        
        if(isExpectedStatus && constantlySeenFrom == null)
            constantlySeenFrom = timestamp;
        
        if(!isExpectedStatus) {
            constantlySeenFrom = null;
            times = 0;
        }
        
        if(raise(timestamp)){
            Optional<String> result;
            
            if((maxTimes != null && times >= maxTimes))
                result = Optional.of("Metric has been in state " 
                                        + expectedStatuses + " for " + times
                                        + " times consecutively.");
            else
                result = Optional.of("Metric has been in state " 
                                        + expectedStatuses + " for " + TimeUtils.toString(getDiff(timestamp))
                                        + ".");
            
            constantlySeenFrom = null;
            times = 0;
            
            return result;
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
        return getDiff(timestamp).compareTo(period) >= 0 || (maxTimes != null && times >= maxTimes);
    }

    @ToString
    @ClassNameAlias("constant-trigger")
    public static class Status_ extends StatusValue{
        private static final long serialVersionUID = -1907347033980904180L;
        
        public Instant constantlySeenFrom;
        public int times;
    }
    
}
