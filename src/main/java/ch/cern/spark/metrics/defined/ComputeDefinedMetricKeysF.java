package ch.cern.spark.metrics.defined;

import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import ch.cern.properties.Properties;
import ch.cern.spark.Pair;
import ch.cern.spark.metrics.Metric;
import scala.Tuple2;

public class ComputeDefinedMetricKeysF implements PairFlatMapFunction<Metric, DefinedMetricStatuskey, Metric> {

	private static final long serialVersionUID = -2525624532462429053L;
	
	private Properties propertiesSourceProps;

	public ComputeDefinedMetricKeysF(Properties propertiesSourceProps) {
		this.propertiesSourceProps = propertiesSourceProps;
	}

	@Override
	public Iterator<Tuple2<DefinedMetricStatuskey, Metric>> call(Metric metric) throws Exception {
		DefinedMetrics.initCache(propertiesSourceProps);
		
        return DefinedMetrics.getCache().get().values().stream()
		        		.filter(definedMetric -> definedMetric.testIfApplyForAnyVariable(metric))
		        		.map(definedMetric -> new Pair<>(definedMetric.getName(), definedMetric.getGroupByMetricIDs(metric.getIDs())))
		        		.filter(pair -> pair.second.isPresent())
		        		.map(pair -> new DefinedMetricStatuskey(pair.first, pair.second.get()))
		        		.map(ids -> new Tuple2<DefinedMetricStatuskey, Metric>(ids, metric))
		        		.iterator();
	}

}
