package ch.cern.utils;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import ch.cern.properties.ConfigurationException;

public class DurationAndTruncate implements Serializable {
    
    private static final long serialVersionUID = -230175194730220401L;

    private Duration duration;
    
    private ChronoUnit truncate;
    
    public DurationAndTruncate(Duration duration) {
        this(duration, ChronoUnit.MILLIS);
    }
    
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((duration == null) ? 0 : duration.hashCode());
        result = prime * result + ((truncate == null) ? 0 : truncate.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        DurationAndTruncate other = (DurationAndTruncate) obj;
        if (duration == null) {
            if (other.duration != null)
                return false;
        } else if (!duration.equals(other.duration))
            return false;
        if (truncate != other.truncate)
            return false;
        return true;
    }

}
