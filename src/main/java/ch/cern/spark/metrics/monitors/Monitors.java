package ch.cern.spark.metrics.monitors;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.trigger.TriggerStatus;
import ch.cern.spark.metrics.trigger.TriggerStatusKey;
import ch.cern.spark.metrics.trigger.ComputeTriggerKeysF;
import ch.cern.spark.metrics.trigger.UpdateTriggerStatusesF;
import ch.cern.spark.metrics.trigger.action.Action;
import ch.cern.spark.status.Status;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;

public class Monitors {

	private transient final static Logger LOG = Logger.getLogger(Monitors.class.getName());
	
	private static Cache<Map<String, Monitor>> cachedMonitors = new Cache<Map<String,Monitor>>() {
		
		@Override
		protected Map<String, Monitor> load() throws Exception {
	        Properties properties = Properties.getCache().get().getSubset("monitor");
	        
	        Set<String> monitorNames = properties.getIDs();
	        
	        Map<String, Monitor> monitors = new HashMap<>();
	        for (String monitorName : monitorNames) {
				Properties monitorProps = properties.getSubset(monitorName);
				
				monitors.put(monitorName, new Monitor(monitorName).config(monitorProps));
			}

	        LOG.info("Loaded Monitors: " + monitors);
	        
	        return monitors;
		}
	};
	
	public static JavaDStream<AnalysisResult> analyze(
	        JavaDStream<Metric> metrics, 
	        Properties propertiesSourceProps, 
	        Optional<JavaDStream<StatusKey>> allStatusesToRemove) 
	                throws Exception {
	    
	    JavaDStream<MonitorStatusKey> statusesToRemove = null;
	    if(allStatusesToRemove.isPresent())
	        statusesToRemove = allStatusesToRemove.get()
	                                .filter(key -> key instanceof MonitorStatusKey)
	                                .map(key -> (MonitorStatusKey) key);
	    
	    JavaPairDStream<MonitorStatusKey, Metric> idAndMetrics = metrics.flatMapToPair(new ComputeMonitorKeysF(propertiesSourceProps));

	    return Status.<MonitorStatusKey, Metric, StatusValue, AnalysisResult>map(
	                    MonitorStatusKey.class, 
	                    StatusValue.class, 
	                    idAndMetrics, 
	                    new UpdateMonitorStatusesF(propertiesSourceProps),
	                    Optional.ofNullable(statusesToRemove)).values();
	}

	public static JavaDStream<Action> applyTriggers(JavaDStream<AnalysisResult> results, Properties propertiesSourceProps, Optional<JavaDStream<StatusKey>> allStatusesToRemove) throws IOException, ClassNotFoundException, ConfigurationException {
	    JavaDStream<TriggerStatusKey> statusesToRemove = null;
	    if(allStatusesToRemove.isPresent())
	        statusesToRemove = allStatusesToRemove.get()
	                                    .filter(key -> key instanceof TriggerStatusKey)
	                                    .map(key -> (TriggerStatusKey) key);
	    
	    JavaPairDStream<TriggerStatusKey, AnalysisResult> idAndAnalysis = results.flatMapToPair(new ComputeTriggerKeysF(propertiesSourceProps));
	    
	    return Status.<TriggerStatusKey, AnalysisResult, TriggerStatus, Action>map(
	                    TriggerStatusKey.class, 
	                    TriggerStatus.class, 
                        idAndAnalysis, 
                        new UpdateTriggerStatusesF(propertiesSourceProps),
                        Optional.ofNullable(statusesToRemove)).values();
	}
	
	public static Cache<Map<String, Monitor>> getCache() {
		return cachedMonitors;
	}

	public static void initCache(Properties propertiesSourceProps) throws ConfigurationException {
		Properties.initCache(propertiesSourceProps);
		
		getCache().setExpiration(Properties.getCache().getExpirationPeriod());
	}

}
