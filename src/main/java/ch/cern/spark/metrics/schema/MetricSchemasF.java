package ch.cern.spark.metrics.schema;

import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.spark.api.java.function.FlatMapFunction;

import ch.cern.properties.Properties;
import ch.cern.spark.json.JSONObject;
import ch.cern.spark.metrics.Metric;

public class MetricSchemasF implements FlatMapFunction<JSONObject, Metric> {

	private static final long serialVersionUID = 116123198242814348L;
	
	private String sourceID;
	
	private Properties propertiesSourceProps;

	private MetricSchema sourceSchema;

	public MetricSchemasF(Properties propertiesSourceProps, String sourceId, MetricSchema sourceSchema) {
		this.sourceID = sourceId;
		this.propertiesSourceProps = propertiesSourceProps;
		this.sourceSchema = sourceSchema;
	}

	@Override
	public Iterator<Metric> call(JSONObject json) throws Exception {
		MetricSchemas.initCache(propertiesSourceProps);
		
		Stream<Metric> metrics = MetricSchemas.getCache().get().values().stream()
												.filter(schema -> schema.containsSource(sourceID))
												.flatMap(schema -> schema.call(json).stream());
		
		metrics = sourceSchema == null ? metrics : Stream.concat(sourceSchema.call(json).stream(), metrics);
		
		return metrics.map(metric -> {
								metric.getIDs().put("$source", sourceID);
								return metric;
							}).iterator();
	}

}
