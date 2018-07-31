package ch.cern.exdemon.metrics.defined;

import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.properties.Properties;
import scala.Tuple2;

public class ComputeDefinedMetricKeysF implements PairFlatMapFunction<Metric, DefinedMetricStatuskey, Metric> {

	private static final long serialVersionUID = -2525624532462429053L;
	
	private Properties propertiesSourceProps;

	public ComputeDefinedMetricKeysF(Properties propertiesSourceProps) {
		this.propertiesSourceProps = propertiesSourceProps;
	}

	@Override
	public Iterator<Tuple2<DefinedMetricStatuskey, Metric>> call(Metric metric) throws Exception {
		ComponentsCatalog.init(propertiesSourceProps);
		
		Map<String, DefinedMetric> definedMetrics = ComponentsCatalog.get(Type.METRIC);
		
        return definedMetrics.values().stream()
		        		.filter(definedMetric -> definedMetric.testIfApplyForAnyVariable(metric))
		        		.map(definedMetric -> new Tuple2<>(definedMetric.getId(), definedMetric.getGroupByAttributes(metric.getAttributes())))
		        		.filter(pair -> pair._2.isPresent())
		        		.map(pair -> new DefinedMetricStatuskey(pair._1, pair._2.get()))
		        		.map(ids -> new Tuple2<DefinedMetricStatuskey, Metric>(ids, metric))
		        		.iterator();
	}

}
