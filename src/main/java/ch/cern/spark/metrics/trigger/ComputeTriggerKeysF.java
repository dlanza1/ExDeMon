package ch.cern.spark.metrics.trigger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.monitors.Monitor;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.trigger.TriggerStatusKey;
import scala.Tuple2;

public class ComputeTriggerKeysF implements PairFlatMapFunction<AnalysisResult, TriggerStatusKey, AnalysisResult> {
    
    private static final long serialVersionUID = 8388632785439398988L;

    private Properties propertiesSourceProperties;
    
    public ComputeTriggerKeysF(Properties propertiesSourceProperties) {
    		this.propertiesSourceProperties = propertiesSourceProperties;
    }

    @Override
    public Iterator<Tuple2<TriggerStatusKey, AnalysisResult>> call(AnalysisResult analysis) throws Exception {
    		Monitors.initCache(propertiesSourceProperties);
    		
    		String monitorID = (String) analysis.getAnalysisParams().get("monitor.name");
    		Optional<Monitor> monitorOpt = Optional.fromNullable(Monitors.getCache().get().get(monitorID));
        if(!monitorOpt.isPresent())
        		return new LinkedList<Tuple2<TriggerStatusKey, AnalysisResult>>().iterator();
        Monitor monitor = monitorOpt.get();
        
        Map<String, String> metricIDs = monitor.getMetricIDs(analysis.getAnalyzed_metric());
        
        Set<String> triggerIDs = monitor.getTriggers().keySet();
        
        return triggerIDs.stream()
        		.map(id -> new TriggerStatusKey(monitorID, id, metricIDs))
        		.map(notID -> new Tuple2<TriggerStatusKey, AnalysisResult>(notID, analysis))
        		.iterator();
    }
    
}
