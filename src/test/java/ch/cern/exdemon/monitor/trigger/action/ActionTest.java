package ch.cern.exdemon.monitor.trigger.action;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;

public class ActionTest {
    
    public static Action DUMMY() {
        Action action = new Action(
                "dummyMonitorID", 
                "dummyTriggerID",
                new HashMap<>(),
                "dummy reason",
                new HashSet<>(),
                new HashMap<>(),
                new AnalysisResult());
        
        action.getTriggeringResult().setAnalyzedMetric(new Metric(Instant.now(), 1234f, new HashMap<>()));
        action.getTriggeringResult().setStatus(Status.OK, "dummy");
        
        return action;
    };

}
