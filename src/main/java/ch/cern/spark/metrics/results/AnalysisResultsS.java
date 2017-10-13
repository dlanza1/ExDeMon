package ch.cern.spark.metrics.results;

import java.io.IOException;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.spark.Properties.Expirable;
import ch.cern.spark.json.JavaObjectToJSONObjectParser;
import ch.cern.spark.json.JsonS;
import ch.cern.spark.metrics.Driver;
import ch.cern.spark.metrics.notifications.NotificationStatusesS;
import ch.cern.spark.metrics.notifications.NotificationStoresRDD;
import ch.cern.spark.metrics.notifications.NotificationsS;
import ch.cern.spark.metrics.notifications.NotificationsWithIdS;
import ch.cern.spark.metrics.notifications.UpdateNotificationStatusesF;
import ch.cern.spark.metrics.notificator.NotificatorID;
import ch.cern.spark.metrics.results.sink.AnalysisResultsSink;

public class AnalysisResultsS extends JavaDStream<AnalysisResult> {

    private static final long serialVersionUID = -7118785350719206804L;
    
    public AnalysisResultsS(JavaDStream<AnalysisResult> stream) {
        super(stream.dstream(), stream.classTag());
    }

    public void sink(AnalysisResultsSink analysisResultsSink) {
        analysisResultsSink.sink(this);
    }
    
    public AnalysisResultsS union(AnalysisResultsS input) {
    		return new AnalysisResultsS(super.union(input));
    }

    public NotificationsS notify(Expirable propertiesExp) throws IOException, ClassNotFoundException {
        JavaPairDStream<NotificatorID, AnalysisResult> analysisWithID = getWithNotificatorID(propertiesExp);
        
        NotificationStoresRDD initialNotificationStores = NotificationStoresRDD.load(
														        		Driver.getCheckpointDir(propertiesExp), 
														        		JavaSparkContext.fromSparkContext(context().sparkContext()));
        
        NotificationStatusesS statuses = UpdateNotificationStatusesF.apply(analysisWithID, propertiesExp, initialNotificationStores);
        
        NotificationsWithIdS allNotificationsStatuses = statuses.getAllNotificationsStatusesWithID();
        allNotificationsStatuses.save(Driver.getCheckpointDir(propertiesExp));
        
        return statuses.getThrownNotifications();
    }

    public JavaPairDStream<NotificatorID, AnalysisResult> getWithNotificatorID(Expirable propertiesExp) {
        return flatMapToPair(new ComputeIDsForAnalysisF(propertiesExp));
    }

    public JsonS asJSON() {
        return new JsonS(JavaObjectToJSONObjectParser.apply(this));
    }

}
