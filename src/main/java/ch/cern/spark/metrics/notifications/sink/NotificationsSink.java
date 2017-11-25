package ch.cern.spark.metrics.notifications.sink;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.Sink;
import ch.cern.spark.metrics.notifications.Notification;

@ComponentType(Type.NOTIFICATIONS_SINK)
public abstract class NotificationsSink extends Component implements Sink<Notification>{

    private static final long serialVersionUID = 8984201586179047078L;
    
    private String id;

    public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public final void sink(Stream<Notification> notifications) {
		notify(notifications.filter(notif -> 
							notif.getSinkIds().contains(id)
							|| notif.getSinkIds().contains("ALL")));
	}

	protected abstract void notify(Stream<Notification> notifications);
	
}
