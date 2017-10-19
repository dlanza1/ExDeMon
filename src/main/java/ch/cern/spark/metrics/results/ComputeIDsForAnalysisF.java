package ch.cern.spark.metrics.results;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import ch.cern.spark.metrics.monitors.Monitor;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.notificator.NotificatorID;
import scala.Tuple2;

public class ComputeIDsForAnalysisF implements PairFlatMapFunction<AnalysisResult, NotificatorID, AnalysisResult> {
    
    private static final long serialVersionUID = 8388632785439398988L;

    private Monitors monitorsCache;

    public ComputeIDsForAnalysisF(Monitors monitorsCache) {
        this.monitorsCache = monitorsCache;
    }

    @Override
    public Iterator<Tuple2<NotificatorID, AnalysisResult>> call(AnalysisResult analysis) throws Exception {
        String monitorID = (String) analysis.getMonitorParams().get("name");
        Map<String, String> metricIDs = analysis.getAnalyzedMetric().getIDs();
        
        java.util.Optional<Monitor> monitorOpt = monitorsCache.get(monitorID);
        if(!monitorOpt.isPresent())
        		return new LinkedList<Tuple2<NotificatorID, AnalysisResult>>().iterator();
        Monitor monitor = monitorOpt.get();
        
        Set<String> notificatorIDs = monitor.getNotificatorIDs();
        
        return notificatorIDs.stream()
        		.map(id -> new NotificatorID(monitorID, id, metricIDs))
        		.map(notID -> new Tuple2<NotificatorID, AnalysisResult>(notID, analysis))
        		.iterator();
    }

}
