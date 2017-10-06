package ch.cern.spark.metrics.results;

import java.io.IOException;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.spark.JavaObjectToJSONObjectParser;
import ch.cern.spark.Properties.Expirable;
import ch.cern.spark.Stream;
import ch.cern.spark.json.JsonS;
import ch.cern.spark.metrics.Driver;
import ch.cern.spark.metrics.notifications.NotificationStatusesS;
import ch.cern.spark.metrics.notifications.NotificationStoresRDD;
import ch.cern.spark.metrics.notifications.NotificationsS;
import ch.cern.spark.metrics.notifications.NotificationsWithIdS;
import ch.cern.spark.metrics.notifications.UpdateNotificationStatusesF;
import ch.cern.spark.metrics.notificator.NotificatorID;
import ch.cern.spark.metrics.results.sink.AnalysisResultsSink;

public class AnalysisResultsS extends Stream<JavaDStream<AnalysisResult>> {

    private static final long serialVersionUID = -7118785350719206804L;
    
    public AnalysisResultsS(JavaDStream<AnalysisResult> stream) {
        super(stream);
    }

    public AnalysisResultsS union(AnalysisResultsS inputStream) {
        return new AnalysisResultsS(stream().union(inputStream.stream()));
    }

    public void sink(AnalysisResultsSink analysisResultsSink) {
        analysisResultsSink.sink(this);
    }

    public NotificationsS notifications(Expirable propertiesExp, NotificationStoresRDD initialNotificationStores) throws IOException {
        JavaPairDStream<NotificatorID, AnalysisResult> metricsWithID = getWithID(propertiesExp);
        
        NotificationStatusesS statuses = UpdateNotificationStatusesF.apply(metricsWithID, propertiesExp, initialNotificationStores);
        
        NotificationsWithIdS allNotificationsStatuses = statuses.getAllNotificationsStatusesWithID();
        allNotificationsStatuses.save(Driver.getCheckpointDir(propertiesExp));
        
        return statuses.getThrownNotifications();
    }

    public JavaPairDStream<NotificatorID, AnalysisResult> getWithID(Expirable propertiesExp) {
        return stream().flatMapToPair(new ComputeIDsForAnalysisF(propertiesExp));
    }

    public JsonS asJSON() {
        return new JsonS(JavaObjectToJSONObjectParser.apply(stream()));
    }

}
