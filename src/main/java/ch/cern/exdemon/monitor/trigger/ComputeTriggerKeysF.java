package ch.cern.exdemon.monitor.trigger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.monitor.Monitor;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.properties.Properties;
import scala.Tuple2;

public class ComputeTriggerKeysF implements PairFlatMapFunction<AnalysisResult, TriggerStatusKey, AnalysisResult> {
    
    private static final long serialVersionUID = 8388632785439398988L;

    private Properties componentsSourceProperties;
    
    public ComputeTriggerKeysF(Properties componentsSourceProperties) {
    		this.componentsSourceProperties = componentsSourceProperties;
    }

    @Override
    public Iterator<Tuple2<TriggerStatusKey, AnalysisResult>> call(AnalysisResult analysis) throws Exception {
        ComponentsCatalog.init(componentsSourceProperties);
    		
    	String monitorID = (String) analysis.getAnalysisParams().get("monitor.name");
    	java.util.Optional<Monitor> monitorOpt = ComponentsCatalog.get(Type.MONITOR, monitorID);
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
