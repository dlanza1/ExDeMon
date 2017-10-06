package ch.cern.spark.metrics.notificator;

import java.io.Serializable;
import java.util.Date;

import ch.cern.spark.Component;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.results.AnalysisResult.Status;

public abstract class Notificator extends Component implements Serializable{

    private static final long serialVersionUID = -5418973482734557441L;
    
    public Notificator() {
        super(Type.NOTIFICATOR);
    }
    
    public Notificator(Class<? extends Component> subClass, String name) {
        super(Type.NOTIFICATOR, subClass, name);
    }

    public abstract Notification process(Status status, Date timestamp);

}
