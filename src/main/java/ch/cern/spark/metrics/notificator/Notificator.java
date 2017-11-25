package ch.cern.spark.metrics.notificator;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;

@ComponentType(Type.NOTIFICATOR)
public abstract class Notificator extends Component implements Function<AnalysisResult, Optional<Notification>>, Serializable{

    private static final long serialVersionUID = -5418973482734557441L;
    
    private Set<String> sinkIDs;
    
    public Notificator() {
	}
    
    @Override
    public void config(Properties properties) throws ConfigurationException {
    		String sinksString = properties.getProperty("sinks", "ALL");
    		sinkIDs = new HashSet<>(Arrays.asList(sinksString.split("\\s")));
    }
    
    public Optional<Notification> apply(AnalysisResult result) {
    		Optional<Notification> notificationOpt = process(result.getStatus(), result.getAnalyzedMetric().getInstant());
    		
    		notificationOpt.ifPresent(notif -> notif.setTags(result.getTags()));
    		notificationOpt.ifPresent(notif -> notif.setSinkIds(sinkIDs));
    		
    		return notificationOpt;
    }

    public abstract Optional<Notification> process(Status status, Instant timestamp);

}
