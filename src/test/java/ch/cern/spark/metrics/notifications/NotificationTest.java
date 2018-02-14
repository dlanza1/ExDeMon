package ch.cern.spark.metrics.notifications;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;

import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;

public class NotificationTest {
    
    public static Notification DUMMY = null;
    static {
        DUMMY = new Notification(
                        Instant.now(), 
                        "dummyMonitorID", 
                        "dummyNotificatorID",
                        new HashMap<>(),
                        "dummy reason",
                        new HashSet<>(),
                        new HashMap<>(),
                        new AnalysisResult());
        DUMMY.getTriggeringResult().setAnalyzedMetric(new Metric(Instant.now(), 1234f, new HashMap<>()));
        DUMMY.getTriggeringResult().setStatus(Status.OK, "dummy");
    }

}
