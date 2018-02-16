package ch.cern.utils;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class ExceptionsCache {

    private Map<Pair<String, Integer>, Instant> raisedAts;
    
    private Duration period;
    
    public ExceptionsCache(Duration period) {
        this.raisedAts = new HashMap<>();
        this.period = period;
    }

    public boolean wasRecentlyRaised(String id, Exception exception) {
        if(exception == null || exception.getMessage() == null)
            return false;
        
        Pair<String, Integer> key = new Pair<>(id, exception.getMessage().hashCode());
        
        Instant raisedAt = raisedAts.get(key);
        
        if(raisedAt == null)
            return false;
        else
            return raisedAt.plus(period).isAfter(Instant.now());
    }

    public void raised(String id, Exception exception) {
        if(exception == null || exception.getMessage() == null)
            return;
        
        Pair<String, Integer> key = new Pair<>(id, exception.getMessage().hashCode());
        
        raisedAts.put(key, Instant.now());
    }

}
