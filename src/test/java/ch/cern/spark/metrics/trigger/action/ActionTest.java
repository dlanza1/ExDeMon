package ch.cern.spark.metrics.trigger.action;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;

import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.metrics.trigger.action.Action;

public class ActionTest {
    
    public static Action DUMMY = null;
    static {
        DUMMY = new Action(
                        Instant.now(), 
                        "dummyMonitorID", 
                        "dummyTriggerID",
                        new HashMap<>(),
                        "dummy reason",
                        new HashSet<>(),
                        new HashMap<>(),
                        new AnalysisResult());
        DUMMY.getTriggeringResult().setAnalyzedMetric(new Metric(Instant.now(), 1234f, new HashMap<>()));
        DUMMY.getTriggeringResult().setStatus(Status.OK, "dummy");
    }

}
