package ch.cern.utils;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import ch.cern.properties.ConfigurationException;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString
@EqualsAndHashCode(callSuper=false)
public class DurationAndTruncate implements Serializable {
    
    private static final long serialVersionUID = -230175194730220401L;

    @Getter
    private Duration duration;
    
    @Getter
    private ChronoUnit truncate;
    
    public DurationAndTruncate(Duration duration) {
        this(duration, ChronoUnit.NANOS);
    }
    
    public DurationAndTruncate(Duration duration, ChronoUnit truncate) {
        this.duration = duration;
        this.truncate = truncate;
    }
    
    public Instant adjustMinus(Instant inputTime) {
        return inputTime.minus(duration).truncatedTo(truncate);
    }
    
    public Instant adjustPlus(Instant inputTime) {
        return inputTime.plus(duration).truncatedTo(truncate);
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
                throw new ConfigurationException(null, "Truncation " + trancConf + " not available.");
            }
        }
        
        return new DurationAndTruncate(duration, truncate);
    }

}
