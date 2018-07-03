package ch.cern.exdemon.monitor;

import java.io.IOException;
import java.util.Optional;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.trigger.ComputeTriggerKeysF;
import ch.cern.exdemon.monitor.trigger.TriggerStatus;
import ch.cern.exdemon.monitor.trigger.TriggerStatusKey;
import ch.cern.exdemon.monitor.trigger.UpdateTriggerStatusesF;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.Status;
import ch.cern.spark.status.StatusOperation;
import ch.cern.spark.status.StatusValue;

public class Monitors {

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

}
