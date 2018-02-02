package ch.cern.spark.metrics.notificator;

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
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import lombok.Getter;
import lombok.ToString;

@ToString
@ComponentType(Type.NOTIFICATOR)
public abstract class Notificator extends Component implements Function<AnalysisResult, Optional<Notification>>, Serializable{

    private static final long serialVersionUID = -5418973482734557441L;
    
    protected Set<String> sinkIDs;
    
    protected Map<String, String> tags;
    
    protected MetricsFilter filter;
    
    private static final String SILENT_PARAM = "silent";
    private static final String SILENT_PERIOD_PARAM = SILENT_PARAM + ".period";
    private static final String SILENT_NOTIFICATOR_PARAM = SILENT_PARAM + ".notificator";
    
    @Getter
    private Duration silentPeriod;
    
    @Getter
    protected Notificator silentPeriodNotificator;
    
    public Notificator() {
	}
    
    @Override
    public void config(Properties properties) throws ConfigurationException {
    		String sinksString = properties.getProperty("sinks", "ALL");
    		sinkIDs = new HashSet<>(Arrays.asList(sinksString.split("\\s")));
    		
    		tags = properties.getSubset("tags").toStringMap();
    		
    		filter = MetricsFilter.build(properties.getSubset("filter"));
    		
    		silentPeriod = properties.getPeriod(SILENT_PERIOD_PARAM, Duration.ofSeconds(0));
    		
    		Properties silentPeriodNotificatorProps = properties.getSubset(SILENT_NOTIFICATOR_PARAM);
    		if(silentPeriodNotificatorProps.isTypeDefined())
    		    silentPeriodNotificator = ComponentManager.build(Type.NOTIFICATOR, "silent-period", silentPeriodNotificatorProps);
    }
    
    public Optional<Notification> apply(AnalysisResult result) {
        if(!filter.test(result.getAnalyzedMetric()))
            return Optional.empty();
        
    		Optional<String> reasonOpt = process(result.getStatus(), result.getAnalyzedMetric().getTimestamp());
    		
    		if(!reasonOpt.isPresent())
    		    return Optional.empty();
    		
    		HashMap<String, String> notificationTags = new HashMap<>(result.getTags());
        notificationTags.putAll(tags);
    		
    		return Optional.of(new Notification(
                		                result.getAnalyzedMetric().getTimestamp(),
                		                "",
                		                getId(),
                		                result.getAnalyzedMetric().getAttributes(),
                		                reasonOpt.get(),
                		                sinkIDs,
                		                notificationTags));
    }

    public abstract Optional<String> process(Status status, Instant timestamp);

}
