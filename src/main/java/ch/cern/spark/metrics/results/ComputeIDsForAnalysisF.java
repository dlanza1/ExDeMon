package ch.cern.spark.metrics.results;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import ch.cern.spark.Properties;
import ch.cern.spark.Properties.Expirable;
import ch.cern.spark.metrics.monitor.Monitor;
import ch.cern.spark.metrics.notificator.NotificatorID;
import scala.Tuple2;

public class ComputeIDsForAnalysisF implements PairFlatMapFunction<AnalysisResult, NotificatorID, AnalysisResult> {
    
    private static final long serialVersionUID = 8388632785439398988L;
    
    private Map<String, Monitor> monitors = null;

    private Properties.Expirable propertiesExp;

    public ComputeIDsForAnalysisF(Expirable propertiesExp) {
        this.propertiesExp = propertiesExp;
    }

    @Override
    public Iterator<Tuple2<NotificatorID, AnalysisResult>> call(AnalysisResult analysis) throws Exception {
        String monitorID = (String) analysis.getMonitorParams().get("name");
        Map<String, String> metricIDs = analysis.getAnalyzedMetric().getIDs();
        
        LinkedList<Tuple2<NotificatorID, AnalysisResult>> tuples = new LinkedList<>();
        
        Monitor monitor = getMonitor(monitorID);
        Set<String> notificatorIDs = monitor.getNotificatorIDs();
        
        for (String id : notificatorIDs) {
            NotificatorID notificatorID = new NotificatorID(monitorID, id, metricIDs);
            
            tuples.add(new Tuple2<NotificatorID, AnalysisResult>(notificatorID, analysis));
        }
        
        return tuples.iterator();
    }
    
    private Monitor getMonitor(String monitorID) {
        if (monitors == null)
            monitors = Monitor.getAll(propertiesExp);

        return monitors.get(monitorID);
    }

}
