package ch.cern.exdemon.metrics.schema;

import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.spark.api.java.function.FlatMapFunction;

import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.json.JSON;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.properties.Properties;

public class MetricSchemasF implements FlatMapFunction<String, Metric> {

	private static final long serialVersionUID = 116123198242814348L;
	
	private String sourceID;
	
	private Properties componentsSourceProperties;

	public MetricSchemasF(Properties componentsSourceProperties, String sourceId) {
		this.sourceID = sourceId;
		this.componentsSourceProperties = componentsSourceProperties;
	}

	@Override
	public Iterator<Metric> call(String jsonString) throws Exception {
		ComponentsCatalog.init(componentsSourceProperties);
		
		JSON jsonObject = new JSON(jsonString);
		
		Map<String, MetricSchema> schemas = ComponentsCatalog.get(Type.SCHEMA);
		
		Stream<Metric> metrics = schemas.values().stream()
											.filter(schema -> schema.containsSource(sourceID))
											.flatMap(schema -> schema.call(jsonObject).stream());
		
		return metrics.map(metric -> {
								metric.getAttributes().put("$source", sourceID);
								return metric;
							}).iterator();
	}

}
