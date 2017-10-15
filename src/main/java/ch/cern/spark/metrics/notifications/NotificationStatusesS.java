package ch.cern.spark.metrics.notifications;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;

import ch.cern.spark.MapWithStateStream;
import ch.cern.spark.metrics.notificator.NotificatorID;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.store.Store;
import scala.Tuple2;

public class NotificationStatusesS extends MapWithStateStream<NotificatorID, AnalysisResult, Store, Notification> {

    private static final long serialVersionUID = 4302779063964080084L;
    
    public NotificationStatusesS(JavaMapWithStateDStream<NotificatorID, AnalysisResult, Store, Notification> stream) {
        super(stream);
    }

    public JavaDStream<Notification> getThrownNotifications() {
        return stream();
    }

    public JavaDStream<Tuple2<NotificatorID, Store>> getAllNotificationsStatusesWithID() {
        return stateSnapshots();
    }
    
}
