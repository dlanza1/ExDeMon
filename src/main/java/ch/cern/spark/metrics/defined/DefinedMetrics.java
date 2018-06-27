package ch.cern.spark.metrics.defined;

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.Cache;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentTypes;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.status.StateDStream;
import ch.cern.spark.status.Status;
import ch.cern.spark.status.StatusOperation;

public class DefinedMetrics {

	private transient final static Logger LOG = Logger.getLogger(DefinedMetrics.class.getName());

    public static final String PARAM = "metrics.define";
	
	private static final Cache<Map<String, DefinedMetric>> cachedDefinedMetrics = new Cache<Map<String,DefinedMetric>>() {
		
		@Override
		protected Map<String, DefinedMetric> load() throws Exception {
	        Properties properties = Properties.getCache().get().getSubset(PARAM);
	        
	        Set<String> metricsDefinedNames = properties.getIDs();
	        
	        Map<String, DefinedMetric> definedMetrics = metricsDefinedNames.stream()
	        		.map(id -> {
                        try {
                            return (DefinedMetric) ComponentTypes.build(Type.METRIC, id, properties.getSubset(id));
                        } catch (ConfigurationException e) {
                            LOG.error(e);
                            
                            return null;
                        }
                    })
	        		.filter(out -> out != null)
	        		.collect(Collectors.toMap(DefinedMetric::getId, m -> m));
	        
	        LOG.info("Metrics defined updated");
	        for (Map.Entry<String, DefinedMetric> definedMetric : definedMetrics.entrySet())
                LOG.info(definedMetric.getKey() + ": " + definedMetric.getValue());
	        
	        return definedMetrics;
		}
	};
	
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
	
	public static Cache<Map<String, DefinedMetric>> getCache() {
		return cachedDefinedMetrics;
	}

	public static void initCache(Properties propertiesSourceProps) throws ConfigurationException {
		Properties.initCache(propertiesSourceProps);
	
		getCache().setExpiration(Properties.getCache().getExpirationPeriod());
	}

}
