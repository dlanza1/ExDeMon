package ch.cern.exdemon.metrics.schema;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.properties.Properties;

public class MetricSchemas {
	
	private transient final static Logger LOG = Logger.getLogger(MetricSchemas.class.getName());
	
	private static final int JSON_MAX_SIZE = 64000;

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
