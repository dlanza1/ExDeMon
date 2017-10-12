package ch.cern.spark.metrics.notifications;

import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.spark.json.JavaObjectToJSONObjectParser;
import ch.cern.spark.json.JsonS;
import ch.cern.spark.metrics.notifications.sink.NotificationsSink;

public class NotificationsS extends JavaDStream<Notification> {

    private static final long serialVersionUID = 5950355219159839954L;
    
    public NotificationsS(JavaDStream<Notification> stream) {
        super(stream.dstream(), stream.classTag());
    }

    public void sink(NotificationsSink notificationsSink) {
        notificationsSink.sink(this);
    }

    public JsonS asJSON() {
        return new JsonS(JavaObjectToJSONObjectParser.apply(this));
    }

}
