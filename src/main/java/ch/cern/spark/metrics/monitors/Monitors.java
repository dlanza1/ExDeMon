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
import ch.cern.spark.metrics.trigger.ComputeTriggerKeysF;
import ch.cern.spark.metrics.trigger.TriggerStatus;
import ch.cern.spark.metrics.trigger.TriggerStatusKey;
import ch.cern.spark.metrics.trigger.UpdateTriggerStatusesF;
import ch.cern.spark.metrics.trigger.action.Action;
import ch.cern.spark.status.Status;
import ch.cern.spark.status.StatusOperation;
import ch.cern.spark.status.StatusValue;

public class Monitors {

	private transient final static Logger LOG = Logger.getLogger(Monitors.class.getName());

    public static final String PARAM = "monitor";
	
	private static Cache<Map<String, Monitor>> cachedMonitors = new Cache<Map<String,Monitor>>() {
		
		@Override
		protected Map<String, Monitor> load() throws Exception {
	        Properties properties = Properties.getCache().get().getSubset(PARAM);
	        
	        Set<String> monitorNames = properties.getIDs();
	        
	        Map<String, Monitor> monitors = new HashMap<>();
	        for (String monitorName : monitorNames) {
				Properties monitorProps = properties.getSubset(monitorName);
				
				monitors.put(monitorName, new Monitor(monitorName).config(monitorProps));
			}

	        LOG.info("Monitors updated");
            for (Map.Entry<String, Monitor> definedMetric : monitors.entrySet())
                LOG.info(definedMetric.getKey() + ": " + definedMetric.getValue());
	        
	        return monitors;
		}
	};
	
	public static JavaDStream<AnalysisResult> analyze(
	        JavaDStream<Metric> metrics, 
	        Properties propertiesSourceProps, 
	        Optional<JavaDStream<StatusOperation<MonitorStatusKey, Metric>>> operationsOpt) 
	                throws Exception {
	    
	    JavaPairDStream<MonitorStatusKey, Metric> metricsKeyed = metrics.flatMapToPair(new ComputeMonitorKeysF(propertiesSourceProps));
	    
	    JavaDStream<StatusOperation<MonitorStatusKey, Metric>> operations = metricsKeyed.map(mk -> new StatusOperation<>(mk._1, mk._2));
	    
	    if(operationsOpt.isPresent())
	        operations = operations.union(operationsOpt.get());

	    return Status.<MonitorStatusKey, Metric, StatusValue, AnalysisResult>map(
	                    MonitorStatusKey.class, 
	                    StatusValue.class, 
	                    operations, 
	                    new UpdateMonitorStatusesF(propertiesSourceProps)).values();
	}

	public static JavaDStream<Action> applyTriggers(
	        JavaDStream<AnalysisResult> results, 
	        Properties propertiesSourceProps, 
	        Optional<JavaDStream<StatusOperation<TriggerStatusKey, AnalysisResult>>> operationsOpt) 
	                throws IOException, ClassNotFoundException, ConfigurationException {
	    
        JavaPairDStream<TriggerStatusKey, AnalysisResult> resultsKeyed = results.flatMapToPair(new ComputeTriggerKeysF(propertiesSourceProps));

        JavaDStream<StatusOperation<TriggerStatusKey, AnalysisResult>> operations = resultsKeyed.map(mk -> new StatusOperation<>(mk._1, mk._2));

        if(operationsOpt.isPresent())
            operations = operations.union(operationsOpt.get());
	    
	    return Status.<TriggerStatusKey, AnalysisResult, TriggerStatus, Action>map(
	                    TriggerStatusKey.class, 
	                    TriggerStatus.class, 
	                    operations, 
                        new UpdateTriggerStatusesF(propertiesSourceProps)).values();
	}
	
	public static Cache<Map<String, Monitor>> getCache() {
		return cachedMonitors;
	}

	public static void initCache(Properties propertiesSourceProps) throws ConfigurationException {
		Properties.initCache(propertiesSourceProps);
		
		getCache().setExpiration(Properties.getCache().getExpirationPeriod());
	}

}
