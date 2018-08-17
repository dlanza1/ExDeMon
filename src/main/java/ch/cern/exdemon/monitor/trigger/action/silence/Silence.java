package ch.cern.exdemon.monitor.trigger.action.silence;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import ch.cern.exdemon.components.Component;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentType;
import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.metrics.filter.MetricsFilter;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.properties.Properties;
import ch.cern.utils.TimeUtils;
import lombok.ToString;

@ToString
@ComponentType(Type.SILENCE)
public class Silence extends Component {

    private static final long serialVersionUID = -9152252903870437450L;
    
    private static DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private Instant from;
    private Instant to;

    private MetricsFilter filter;

    public Silence() {
    }
    
    @Override
    protected ConfigurationResult config(Properties properties) {
        ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
        
        String fromAsString = properties.getProperty("from");
        if(fromAsString != null)
            try {
                LocalDateTime localDateTime = LocalDateTime.from(dateTimeFormatter.parse(fromAsString));
                ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
                from = Instant.from(zonedDateTime);
            }catch(Exception e){
                confResult.withError("from", "could not be parsed, expect format \"yyyy-MM-dd HH:mm:ss\" (e.g: 2016-02-16 11:00:02)");
            }
        
        String toAsString = properties.getProperty("to");
        if(toAsString != null)
            try {
                LocalDateTime localDateTime = LocalDateTime.from(dateTimeFormatter.parse(toAsString));
                ZonedDateTime zonedDateTime = ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
                to = Instant.from(zonedDateTime);
            }catch(Exception e){
                confResult.withError("to", "could not be parsed, expect format \""+dateTimeFormatter.toString()+"\" (e.g: 2016-03-16 11:00:02)");
            }
        
        String durationAsString = properties.getProperty("duration");
        if(durationAsString != null) {
            try {
                Duration duration = TimeUtils.parsePeriod(durationAsString);
                
                if(to != null)
                    confResult.withWarning("to", "when using duration, this configuration is ignored");
                
                if(from == null)
                    confResult.withError("from", "when using duration, creation must be configured");
                else
                    to = from.plus(duration);
            }catch(Exception e){
                confResult.withError("duration", "wrong format, must be like 15m, 2h, 2w");
            }
        }
    
        filter = new MetricsFilter();
        confResult.merge("filter", filter.config(properties.getSubset("filter")));
        
        return confResult;
    }
    
    public boolean isActiveAt(Instant time) {
        if(from != null && time.isBefore(from))
            return false;
        
        if(to != null && time.isAfter(to))
            return false;
        
        return true;
    }

    public boolean filter(Action action) {
        if(!isActiveAt(action.getCreation_timestamp()))
            return true;
        
        return !filter.test(action.getMetric_attributes());
    }
    
}
