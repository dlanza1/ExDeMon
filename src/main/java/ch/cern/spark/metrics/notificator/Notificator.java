package ch.cern.spark.metrics.notificator;

import java.io.Serializable;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Function;

import ch.cern.Component;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;

public abstract class Notificator extends Component implements Function<AnalysisResult, Optional<Notification>>, Serializable{

    private static final long serialVersionUID = -5418973482734557441L;
    
    public Notificator() {
        super(Type.NOTIFICATOR);
    }
    
    public Notificator(Class<? extends Component> subClass, String name) {
        super(Type.NOTIFICATOR, subClass, name);
    }
    
    public Optional<Notification> apply(AnalysisResult result) {
    		return process(result.getStatus(), result.getAnalyzedMetric().getInstant());
    }

    public abstract Optional<Notification> process(Status status, Instant timestamp);

}
