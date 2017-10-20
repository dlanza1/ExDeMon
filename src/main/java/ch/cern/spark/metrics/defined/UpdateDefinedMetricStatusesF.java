package ch.cern.spark.metrics.defined;

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
		
		java.util.Optional<DefinedMetric> definedMetricOpt = definedMetrics.get(id.getDefinedMetricName());
		if(!definedMetricOpt.isPresent()) {
			status.remove();
			return Optional.empty();
		}
		DefinedMetric definedMetric = definedMetricOpt.get();
			
		DefinedMetricStore store = getStore(status);
		
		Metric metric = metricOpt.get();
		
		definedMetric.updateStore(store, metric);
		
		Optional<Metric> newMetric = toOptional(definedMetric.generateByUpdate(store, metric, id.getGroupByMetricIDs()));
		
		status.update(store);
		
		return newMetric;
	}

	private Optional<Metric> toOptional(java.util.Optional<Metric> javaOptional) {
		return javaOptional.isPresent() ? Optional.of(javaOptional.get()) : Optional.empty();
	}

	private DefinedMetricStore getStore(State<DefinedMetricStore> status) {
		return status.exists() ? status.get() : new DefinedMetricStore();
	}

}
