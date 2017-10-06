package ch.cern.spark.metrics.notifications.sink;

import ch.cern.spark.Component;
import ch.cern.spark.metrics.notifications.NotificationsS;

public abstract class NotificationsSink extends Component{

    private static final long serialVersionUID = 8984201586179047078L;
    
    public NotificationsSink() {
        super(Type.NOTIFICATIONS_SINK);
    }
    
    public NotificationsSink(Class<? extends Component> subClass, String name) {
        super(Type.NOTIFICATIONS_SINK, subClass, name);
    }

    public abstract void sink(NotificationsS notifications);
    
}
