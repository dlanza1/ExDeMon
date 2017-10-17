package ch.cern.spark.metrics.defined;

import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import ch.cern.spark.metrics.Metric;
import scala.Tuple2;

public class ComputeIDsForDefinedMetricsF implements PairFlatMapFunction<Metric, DefinedMetricID, Metric> {

	private static final long serialVersionUID = -2525624532462429053L;
	
	private DefinedMetrics definedMetricsCache;

	public ComputeIDsForDefinedMetricsF(DefinedMetrics definedMetricsCache) {
		this.definedMetricsCache = definedMetricsCache;
	}

	@Override
	public Iterator<Tuple2<DefinedMetricID, Metric>> call(Metric metric) throws Exception {
        return definedMetricsCache.get().values().stream()
			        		.filter(definedMetric -> definedMetric.testIfAnyFilter(metric))
			        		.map(definedMetric -> new DefinedMetricID(definedMetric.getName(), definedMetric.getGruopByMetricIDs(metric.getIDs())))
			        		.map(ids -> new Tuple2<DefinedMetricID, Metric>(ids, metric))
			        		.iterator();
	}

}
