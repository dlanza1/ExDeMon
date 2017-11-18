package ch.cern.spark.metrics.defined.equation.var;

import java.time.Duration;
import java.util.Optional;
import java.util.function.Predicate;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.DefinedMetricStore;
import ch.cern.spark.metrics.defined.equation.ValueComputable;
import ch.cern.spark.metrics.filter.MetricsFilter;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.StringValue;
import ch.cern.spark.metrics.value.Value;

public abstract class MetricVariable implements ValueComputable, Predicate<Metric> {

	protected String name;
	
	private MetricsFilter filter;

	protected Duration expirePeriod;

	public MetricVariable(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}

	public MetricVariable config(Properties properties) throws ConfigurationException {
		filter = MetricsFilter.build(properties.getSubset("filter"));
		
		if(properties.containsKey("expire") && properties.getProperty("expire").toLowerCase().equals("never"))
			expirePeriod = null;
		else
			expirePeriod = properties.getPeriod("expire", Duration.ofMinutes(10));
		
		return this;
	}

	@Override
	public boolean test(Metric metric) {
		return filter.test(metric);
	}

	public abstract void updateStore(DefinedMetricStore store, Metric metric);
	
	public static Optional<Class<? extends MetricVariable>> returnTypeFromAggregation(Properties metricVariableProperties) throws ConfigurationException{
		String aggregateVal = metricVariableProperties.getProperty("aggregate");
		
		if(aggregateVal == null)
			return Optional.empty();
		
		aggregateVal = aggregateVal.toUpperCase();
		
		try {
			FloatMetricVariable.Operation.valueOf(aggregateVal);
			return Optional.of(FloatMetricVariable.class);
		}catch(IllegalArgumentException e) {}
		try {
			StringMetricVariable.Operation.valueOf(aggregateVal);
			return Optional.of(StringMetricVariable.class);
		}catch(IllegalArgumentException e) {}
		try {
			BooleanMetricVariable.Operation.valueOf(aggregateVal);
			return Optional.of(BooleanMetricVariable.class);
		}catch(IllegalArgumentException e) {}
		
		throw new ConfigurationException("Aggregation function " + aggregateVal + " does not exist.");
	}
	
	public static Class<? extends MetricVariable> from(Class<? extends Value> valueType) {
		if(valueType.equals(FloatValue.class))
			return FloatMetricVariable.class;
		if(valueType.equals(StringValue.class))
			return StringMetricVariable.class;
		if(valueType.equals(BooleanValue.class))
			return BooleanMetricVariable.class;
		return AnyMetricVariable.class;
	}
	
}
