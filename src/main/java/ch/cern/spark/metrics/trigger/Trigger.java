package ch.cern.spark.metrics.trigger;

import java.io.Serializable;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.components.ComponentType;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.filter.MetricsFilter;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.metrics.trigger.action.Action;
import lombok.Getter;
import lombok.ToString;

@ToString
@ComponentType(Type.TRIGGER)
public abstract class Trigger extends Component implements Function<AnalysisResult, Optional<Action>>, Serializable{

    private static final long serialVersionUID = -5418973482734557441L;
    
    protected Set<String> actuatorIDs;
    
    protected Map<String, String> tags;
    
    protected MetricsFilter filter;
    
    private static final String SILENT_PARAM = "silent";
    private static final String SILENT_PERIOD_PARAM = SILENT_PARAM + ".period";
    private static final String SILENT_TRIGGER_PARAM = SILENT_PARAM + ".trigger";
    
    @Getter
    private Duration silentPeriod;
    
    @Getter
    protected Trigger silentPeriodTrigger;
    
    public Trigger() {
	}
    
    @Override
    public void config(Properties properties) throws ConfigurationException {
    		String actuatorIDsString = properties.getProperty("actuators");
    		
    		//TODO backward compatibility
    		if(actuatorIDsString == null)
    		    actuatorIDsString = properties.getProperty("sinks");
    		//TODO backward compatibility
    		
    		if(actuatorIDsString != null)
    		    actuatorIDs = new HashSet<>(Arrays.asList(actuatorIDsString.split("\\s")));
    		else
    		    actuatorIDs = new HashSet<>();
    		
    		tags = properties.getSubset("tags").toStringMap();
    		
    		filter = MetricsFilter.build(properties.getSubset("filter"));
    		
    		silentPeriod = properties.getPeriod(SILENT_PERIOD_PARAM, Duration.ofSeconds(0));
    		
    		Properties silentPeriodTriggerProps = properties.getSubset(SILENT_TRIGGER_PARAM);
    		//TODO backward compatibility
    		silentPeriodTriggerProps.putAll(properties.getSubset(SILENT_PARAM + ".notificator"));
    		//TODO backward compatibility
    		
    		if(silentPeriodTriggerProps.isTypeDefined())
    		    silentPeriodTrigger = ComponentManager.build(Type.TRIGGER, "silent-period", silentPeriodTriggerProps);
    }
    
    public Optional<Action> apply(AnalysisResult result) {
        if(!filter.test(result.getAnalyzed_metric()))
            return Optional.empty();
        
    		Optional<String> reasonOpt = process(result.getStatus(), result.getAnalyzed_metric().getTimestamp());
    		
    		if(!reasonOpt.isPresent())
    		    return Optional.empty();
    		
    		HashMap<String, String> triggerTags = new HashMap<>(result.getTags());
        triggerTags.putAll(tags);
    		
    		return Optional.of(new Action(
                		                result.getAnalyzed_metric().getTimestamp(),
                		                "",
                		                getId(),
                		                result.getAnalyzed_metric().getAttributes(),
                		                reasonOpt.get(),
                		                actuatorIDs,
                		                triggerTags,
                		                result));
    }

    public abstract Optional<String> process(Status status, Instant timestamp);

}
