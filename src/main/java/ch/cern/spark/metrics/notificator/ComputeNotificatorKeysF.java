package ch.cern.spark.metrics.notificator;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.monitors.Monitor;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.notificator.NotificatorStatusKey;
import ch.cern.spark.metrics.results.AnalysisResult;
import scala.Tuple2;

public class ComputeNotificatorKeysF implements PairFlatMapFunction<AnalysisResult, NotificatorStatusKey, AnalysisResult> {
    
    private static final long serialVersionUID = 8388632785439398988L;

    private Properties propertiesSourceProperties;
    
    public ComputeNotificatorKeysF(Properties propertiesSourceProperties) {
    		this.propertiesSourceProperties = propertiesSourceProperties;
    }

    @Override
    public Iterator<Tuple2<NotificatorStatusKey, AnalysisResult>> call(AnalysisResult analysis) throws Exception {
    		Monitors.initCache(propertiesSourceProperties);
    		
    		String monitorID = (String) analysis.getAnalysisParams().get("monitor.name");
    		Optional<Monitor> monitorOpt = Optional.fromNullable(Monitors.getCache().get().get(monitorID));
        if(!monitorOpt.isPresent())
        		return new LinkedList<Tuple2<NotificatorStatusKey, AnalysisResult>>().iterator();
        Monitor monitor = monitorOpt.get();
        
        Map<String, String> metricIDs = monitor.getMetricIDs(analysis.getAnalyzed_metric());
        
        Set<String> notificatorIDs = monitor.getNotificators().keySet();
        
        return notificatorIDs.stream()
        		.map(id -> new NotificatorStatusKey(monitorID, id, metricIDs))
        		.map(notID -> new Tuple2<NotificatorStatusKey, AnalysisResult>(notID, analysis))
        		.iterator();
    }
    
}
