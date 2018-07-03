package ch.cern.exdemon.metrics.defined;

import java.io.IOException;
import java.util.Optional;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.StateDStream;
import ch.cern.spark.status.Status;
import ch.cern.spark.status.StatusOperation;

public class DefinedMetrics {

	public static JavaDStream<Metric> generate(
	        JavaDStream<Metric> metrics, 
	        Properties propertiesSourceProps, 
	        Optional<JavaDStream<StatusOperation<DefinedMetricStatuskey, Metric>>> operationsOpt) 
	                throws ClassNotFoundException, IOException, ConfigurationException{
	    
        JavaPairDStream<DefinedMetricStatuskey, Metric> resultsKeyed = metrics.flatMapToPair(new ComputeDefinedMetricKeysF(propertiesSourceProps));

        JavaDStream<StatusOperation<DefinedMetricStatuskey, Metric>> operations = resultsKeyed.map(mk -> new StatusOperation<>(mk._1, mk._2));

        if(operationsOpt.isPresent())
            operations = operations.union(operationsOpt.get());
        
        StateDStream<DefinedMetricStatuskey, Metric, VariableStatuses, Metric> statuses = 
                Status.<DefinedMetricStatuskey, Metric, VariableStatuses, Metric>map(
                        DefinedMetricStatuskey.class, 
                        VariableStatuses.class, 
                        operations, 
                        new UpdateDefinedMetricStatusesF(propertiesSourceProps));

		JavaDStream<Metric> definedMetricsWhenBatch = statuses.statuses().transform((rdd, time) -> rdd.flatMap(new ComputeBatchDefineMetricsF(time, propertiesSourceProps)));
        
        return statuses.values().union(definedMetricsWhenBatch); 
	}

}
