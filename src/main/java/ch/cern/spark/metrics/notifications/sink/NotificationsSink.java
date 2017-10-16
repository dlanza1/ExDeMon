package ch.cern.spark.metrics.notifications.sink;

import ch.cern.Component;
import ch.cern.spark.metrics.Sink;
import ch.cern.spark.metrics.notifications.Notification;

public abstract class NotificationsSink extends Component implements Sink<Notification>{

    private static final long serialVersionUID = 8984201586179047078L;
    
    public NotificationsSink() {
        super(Type.NOTIFICATIONS_SINK);
    }
    
    public NotificationsSink(Class<? extends Component> subClass, String name) {
        super(Type.NOTIFICATIONS_SINK, subClass, name);
    }

}
