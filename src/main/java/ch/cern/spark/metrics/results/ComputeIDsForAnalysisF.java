package ch.cern.spark.metrics.results;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import ch.cern.spark.Properties;
import ch.cern.spark.Properties.PropertiesCache;
import ch.cern.spark.metrics.monitor.Monitor;
import ch.cern.spark.metrics.notificator.NotificatorID;
import scala.Tuple2;

public class ComputeIDsForAnalysisF implements PairFlatMapFunction<AnalysisResult, NotificatorID, AnalysisResult> {
    
    private static final long serialVersionUID = 8388632785439398988L;
    
    private Map<String, Monitor> monitors = null;

    private Properties.PropertiesCache propertiesExp;

    public ComputeIDsForAnalysisF(PropertiesCache propertiesExp) {
        this.propertiesExp = propertiesExp;
    }

    @Override
    public Iterator<Tuple2<NotificatorID, AnalysisResult>> call(AnalysisResult analysis) throws Exception {
        String monitorID = (String) analysis.getMonitorParams().get("name");
        Map<String, String> metricIDs = analysis.getAnalyzedMetric().getIDs();
        
        Monitor monitor = getMonitor(monitorID);
        Set<String> notificatorIDs = monitor.getNotificatorIDs();
        
        return notificatorIDs.stream()
        		.map(id -> new NotificatorID(monitorID, id, metricIDs))
        		.map(notID -> new Tuple2<NotificatorID, AnalysisResult>(notID, analysis))
        		.iterator();
    }
    
    private Monitor getMonitor(String monitorID) throws IOException {
        if (monitors == null)
            monitors = Monitor.getAll(propertiesExp);

        return monitors.get(monitorID);
    }

}
