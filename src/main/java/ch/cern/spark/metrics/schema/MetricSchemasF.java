package ch.cern.spark.metrics.schema;

import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.spark.api.java.function.FlatMapFunction;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;

public class MetricSchemasF implements FlatMapFunction<String, Metric> {

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
	public Iterator<Metric> call(String jsonString) throws Exception {
		MetricSchemas.initCache(propertiesSourceProps);
		
		Stream<Metric> metrics = MetricSchemas.getCache().get().values().stream()
												.filter(schema -> schema.containsSource(sourceID))
												.flatMap(schema -> schema.call(jsonString).stream());
		
		metrics = sourceSchema == null ? metrics : Stream.concat(sourceSchema.call(jsonString).stream(), metrics);
		
		return metrics.map(metric -> {
								metric.getAttributes().put("$source", sourceID);
								return metric;
							}).iterator();
	}

}
