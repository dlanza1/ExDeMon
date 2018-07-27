package ch.cern.exdemon.monitor.trigger.types;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
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
import ch.cern.utils.Pair;
import ch.cern.utils.TimeUtils;
import lombok.ToString;

@ToString
@RegisterComponentType("percentage")
public class PercentageTrigger extends Trigger implements HasStatus {
    
    private static final long serialVersionUID = -7890231998987060652L;
    
    private static final String STATUSES_PARAM = "statuses";
    private Set<Status> expectedStatuses;
    
    private static final String PERIOD_PARAM = "period";
    private static final Duration PERIOD_DEFAULT = Duration.ofMinutes(15);
    private Duration period = PERIOD_DEFAULT;
    
    private static final String PERCENTAGE_PARAM = "percentage";
    private static final String PERCENTAGE_DEFAULT = "90";
    private float percentage;
    
    private List<Pair<Instant, Boolean>> hits;
    
    public PercentageTrigger() {
        hits = new LinkedList<>();
    }

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
        
        String percentage_s = properties.getProperty(PERCENTAGE_PARAM, PERCENTAGE_DEFAULT);
        percentage = Float.valueOf(percentage_s);
        
        return configResult.merge(null, properties.warningsIfNotAllPropertiesUsed());
    }
    
    @Override
    public void load(StatusValue store) {
        if(store == null || !(store instanceof Status_))
            return;
        
        Status_ data = (Status_) store;
        
        hits = data.hits;
    }

    @Override
    public StatusValue save() {
        Status_ store = new Status_();
        
        store.hits = hits;
        
        return store;
    }

    @Override
    public Optional<String> process(Status status, Instant timestamp) {
        removeExpiredHits(timestamp);
        
        hits.add(new Pair<Instant, Boolean>(timestamp, isExpectedStatus(status)));
        
        Duration coveredPeriod = getCoveredPeriod(timestamp);
        if(coveredPeriod.compareTo(period) < 0)
            return Optional.empty();
        
        if(raise(timestamp)){
            hits = new LinkedList<>();
            
            return Optional.of("Metric has been " + percentage + "% of the last "
                                    + TimeUtils.toString(period) + " in state " + expectedStatuses + ".");
        }else{
            return Optional.empty();
        }
    }

    private Duration getCoveredPeriod(Instant timestamp) {
    		Instant oldestTimestamp = getOldestTimestamp();
    		
        if(oldestTimestamp == null)
            return Duration.ZERO;
        
        return Duration.between(oldestTimestamp, timestamp).abs();
    }

    private Instant getOldestTimestamp() {
    		Instant oldestTimestamp = null;
        
        for (Pair<Instant, Boolean> hit : hits)
            if(oldestTimestamp == null || oldestTimestamp.compareTo(hit.first) > 0)
                oldestTimestamp = hit.first;
        
        return oldestTimestamp;
    }

    private void removeExpiredHits(Instant timestamp) {
        // It needs to store a longer period than the configured period
        // Because percentage is calculated for, as minimum, the period
        Duration extendedPeriod = Duration.ofSeconds((long) (period.getSeconds() * 1.2));
        
        Instant expirationTime = timestamp.minus(extendedPeriod);
        
        Iterator<Pair<Instant, Boolean>> it = hits.iterator();
        while(it.hasNext())
            if(it.next().first.isBefore(expirationTime))
                it.remove();
    }

    private boolean isExpectedStatus(Status status) {
        return expectedStatuses.contains(status);
    }

    private boolean raise(Instant currentTime) {
        float percentageOfHits = computePercentageOfHits(currentTime);
        
        return percentageOfHits > (percentage / 100f);
    }

    private float computePercentageOfHits(Instant currentTime) {
        float counter_true = 0;
        float counter = 0;
        
        for (Pair<Instant, Boolean> pair : hits)
            if(isInPeriod(currentTime, pair.first)){
                counter++;
                
                if(pair.second)
                    counter_true++;
            }
        
        return counter_true / counter;
    }

    private boolean isInPeriod(Instant currentTime, Instant hitTime) {
    		Instant oldestTime = currentTime.minus(period);
        
        return hitTime.isAfter(oldestTime) && hitTime.isBefore(currentTime)
        			|| hitTime.equals(oldestTime) || hitTime.equals(currentTime);
    }

    @ToString
    @ClassNameAlias("percentage-trigger")
    public static class Status_ extends StatusValue{

		private static final long serialVersionUID = -1907347033980904180L;
        
        List<Pair<Instant, Boolean>> hits;
    }

}
