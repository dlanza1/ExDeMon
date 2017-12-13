package ch.cern.utils;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import ch.cern.properties.ConfigurationException;

public class DurationAndTruncate {
    
    private Duration duration;
    
    private ChronoUnit truncate;
    
    public DurationAndTruncate(Duration duration, ChronoUnit truncate) {
        this.duration = duration;
        this.truncate = truncate;
    }
    
    public Instant adjust(Instant inputTime) {
        return inputTime.minus(duration).truncatedTo(truncate);
    }
    
    public static DurationAndTruncate from(String config) throws ConfigurationException {
        int commaIndex = config.indexOf(",");
        
        Duration duration = null;
        
        if(commaIndex < 0)
            duration = TimeUtils.parsePeriod(config);
        else
            duration = TimeUtils.parsePeriod(config.substring(0, commaIndex));
        
        ChronoUnit truncate = ChronoUnit.NANOS;
        
        if(commaIndex > 0) {
            String trancConf = config.substring(commaIndex + 1).trim();
            
            switch(trancConf) {
            case "d":
                truncate = ChronoUnit.DAYS;
                break;
            case "h":
                truncate = ChronoUnit.HOURS;
                break;
            case "m":
                truncate = ChronoUnit.MINUTES;
                break;
            default:
                throw new ConfigurationException("Truncation " + trancConf + " not available.");
            }
        }
        
        return new DurationAndTruncate(duration, truncate);
    }

    public Duration getDuration() {
        return duration;
    }
    
    public ChronoUnit getTruncate() {
        return truncate;
    }

    @Override
    public String toString() {
        return "DurationAndTruncate [duration=" + duration + ", truncate=" + truncate + "]";
    }

}
