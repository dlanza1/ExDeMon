package ch.cern.spark.metrics.schema;

import java.util.Collections;
import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.FlatMapFunction;

import ch.cern.properties.Properties;
import ch.cern.spark.json.JSON;
import ch.cern.spark.metrics.Metric;

public class MetricSchemasF implements FlatMapFunction<String, Metric> {

	private static final long serialVersionUID = 116123198242814348L;
	
	private transient final static Logger LOG = Logger.getLogger(MetricSchemasF.class.getName());
	
	private String sourceID;
	
	private Properties propertiesSourceProps;

	public MetricSchemasF(Properties propertiesSourceProps, String sourceId) {
		this.sourceID = sourceId;
		this.propertiesSourceProps = propertiesSourceProps;
	}

	@Override
	public Iterator<Metric> call(String jsonString) throws Exception {
		MetricSchemas.initCache(propertiesSourceProps);
		
		if(jsonString.length() > 64000) {
		    LOG.warn("Event dropped because exceeds max size (64000): " + jsonString.substring(0, 10000) + "...");
		    return Collections.<Metric>emptyList().iterator();
		}
		
		JSON jsonObject = new JSON(jsonString);
		
		Stream<Metric> metrics = MetricSchemas.getCache().get().values().stream()
												.filter(schema -> schema.containsSource(sourceID))
												.flatMap(schema -> schema.call(jsonObject).stream());
		
		return metrics.map(metric -> {
								metric.getAttributes().put("$source", sourceID);
								return metric;
							}).iterator();
	}

}
