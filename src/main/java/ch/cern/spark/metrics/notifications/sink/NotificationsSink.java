package ch.cern.spark.metrics.notifications.sink;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.spark.metrics.Sink;
import ch.cern.spark.metrics.notifications.Notification;

@ComponentType(Type.NOTIFICATIONS_SINK)
public abstract class NotificationsSink extends Component implements Sink<Notification>{

    private static final long serialVersionUID = 8984201586179047078L;

}
