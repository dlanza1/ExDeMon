package ch.cern.spark.metrics.defined;

import java.util.Set;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

import ch.cern.spark.metrics.Metric;

public class UpdateDefinedMetricStatusesF 
	implements Function4<Time, DefinedMetricID, Optional<Metric>, State<DefinedMetricStore>, Optional<Metric>> {

	private static final long serialVersionUID = 2965182980222300453L;
	
	private DefinedMetrics definedMetrics;

	public UpdateDefinedMetricStatusesF(DefinedMetrics definedMetrics) {
		this.definedMetrics = definedMetrics;
	}

	@Override
	public Optional<Metric> call(Time time, DefinedMetricID id, Optional<Metric> metricOpt, State<DefinedMetricStore> status)
			throws Exception {

		if(status.isTimingOut() || !metricOpt.isPresent())
			return Optional.empty();
		
		DefinedMetric definedMetric = definedMetrics.get().get(id.getDefinedMetricName());
		DefinedMetricStore store = getStore(status);
		
		Metric metric = metricOpt.get();
		
		Set<String> metricIDs = definedMetric.getMetricIDs(metric);
		for (String metricID : metricIDs)
			store.updateValue(metricID, metric.getValue());
		status.update(store);
		
		return definedMetric.generate(store, metric, id.getGroupByMetricIDs());
	}

	private DefinedMetricStore getStore(State<DefinedMetricStore> status) {
		return status.exists() ? status.get() : new DefinedMetricStore();
	}

}
