package ch.cern.spark.metrics.defined;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;

public class UpdateDefinedMetricStatusesF 
	implements Function4<Time, DefinedMetricStatuskey, Optional<Metric>, State<VariableStatuses>, Optional<Metric>> {

	private static final long serialVersionUID = 2965182980222300453L;

	private Properties propertiesSourceProps;

	public UpdateDefinedMetricStatusesF(Properties propertiesSourceProps) {
		this.propertiesSourceProps = propertiesSourceProps;
	}

	@Override
	public Optional<Metric> call(Time time, DefinedMetricStatuskey id, Optional<Metric> metricOpt, State<VariableStatuses> status)
			throws Exception {

		if(status.isTimingOut() || !metricOpt.isPresent())
			return Optional.empty();
		
		DefinedMetrics.initCache(propertiesSourceProps);
		
		Optional<DefinedMetric> definedMetricOpt = Optional.fromNullable(DefinedMetrics.getCache().get().get(id.getDefinedMetricName()));
		if(!definedMetricOpt.isPresent()) {
			status.remove();
			return Optional.empty();
		}
		DefinedMetric definedMetric = definedMetricOpt.get();
			
		VariableStatuses store = getStore(status);
		
		Metric metric = metricOpt.get();
		
		definedMetric.updateStore(store, metric, id.getGroupByMetricIDs().keySet());
		
		Optional<Metric> newMetric = toOptional(definedMetric.generateByUpdate(store, metric, id.getGroupByMetricIDs()));
		
		store.update(status, time);
		
		return newMetric;
	}

	private Optional<Metric> toOptional(java.util.Optional<Metric> javaOptional) {
		return javaOptional.isPresent() ? Optional.of(javaOptional.get()) : Optional.empty();
	}

	private VariableStatuses getStore(State<VariableStatuses> status) {
		return status.exists() ? status.get() : new VariableStatuses();
	}

}
