package ch.cern.spark.metrics.notifications;

import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;

import ch.cern.spark.Stream;
import ch.cern.spark.metrics.notificator.NotificatorID;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.store.Store;

public class NotificationStatusesS extends Stream<JavaMapWithStateDStream<NotificatorID, AnalysisResult, Store, Notification>> {

    private static final long serialVersionUID = 4302779063964080084L;
    
    public NotificationStatusesS(JavaMapWithStateDStream<NotificatorID, AnalysisResult, Store, Notification> stream) {
        super(stream);
    }

    public NotificationsS getThrownNotifications() {
        return new NotificationsS(stream());
    }

    public NotificationsWithIdS getAllNotificationsStatusesWithID() {
        return new NotificationsWithIdS(stream().stateSnapshots());
    }
    
}
