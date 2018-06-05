package ch.cern.spark.metrics.schema;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;

public class MetricSchemas {
	
	private transient final static Logger LOG = Logger.getLogger(MetricSchemas.class.getName());
	
	public static final String PARAM = "metrics.schema";
	
	private static final int JSON_MAX_SIZE = 64000;
	
	private static final Cache<Map<String, MetricSchema>> cachedMetricSchemas = new Cache<Map<String, MetricSchema>>() {

        @Override
		protected Map<String, MetricSchema> load() throws Exception {
	        Properties properties = Properties.getCache().get().getSubset(PARAM);
	        
	        Set<String> metricSchemaIDs = properties.getIDs();
	        
	        Map<String, MetricSchema> metricSchemas = metricSchemaIDs.stream()
	        		.map(id -> new MetricSchema(id).config(properties.getSubset(id)))
	        		.filter(out -> out != null)
	        		.collect(Collectors.toMap(MetricSchema::getId, m -> m));
	        
	        LOG.info("Dynamic Metric schemas: " + metricSchemas);
	        
	        return metricSchemas;
		}
	};
	
	public static Cache<Map<String, MetricSchema>> getCache() {
		return cachedMetricSchemas;
	}

	public static void initCache(Properties propertiesSourceProps) throws ConfigurationException {
		Properties.initCache(propertiesSourceProps);
	
		getCache().setExpiration(Properties.getCache().getExpirationPeriod());
	}

	public static JavaDStream<Metric> generate(JavaDStream<String> jsons, Properties propertiesSourceProps, String sourceId) {
	    jsons = jsons.filter(string -> {
                	        if(string.length() > JSON_MAX_SIZE) {
                	            LOG.warn("Event dropped because exceeds max size ("+JSON_MAX_SIZE+"): " + string.substring(0, 10000) + "...");
                	            
                	            return false;
                	        }
                	        
                	        return true;
                	    });
	    
		return jsons.flatMap(new MetricSchemasF(propertiesSourceProps, sourceId));
	}

}
