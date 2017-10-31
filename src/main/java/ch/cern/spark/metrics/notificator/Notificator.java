package ch.cern.spark.metrics.notificator;

import java.io.Serializable;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;

@ComponentType(Type.NOTIFICATOR)
public abstract class Notificator extends Component implements Function<AnalysisResult, Optional<Notification>>, Serializable{

    private static final long serialVersionUID = -5418973482734557441L;
    
    public Optional<Notification> apply(AnalysisResult result) {
    		Optional<Notification> notificationOpt = process(result.getStatus(), result.getAnalyzedMetric().getInstant());
    		
    		notificationOpt.ifPresent(notif -> notif.setTags(result.getTags()));
    		
    		return notificationOpt;
    }

    public abstract Optional<Notification> process(Status status, Instant timestamp);

}
