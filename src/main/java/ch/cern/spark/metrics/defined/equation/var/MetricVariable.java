package ch.cern.spark.metrics.defined.equation.var;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.filter.MetricsFilter;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.StringValue;
import ch.cern.spark.metrics.value.Value;

public abstract class MetricVariable extends Variable {
	
	private MetricsFilter filter;

	protected Duration expirePeriod;

	public MetricVariable(String name) {
		super(name);
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

	@Override
	public Value compute(VariableStores variableStores, Instant time) {
		MetricVariableStore store = null;
		if(variableStores.containsKey(name) && variableStores.get(name) instanceof MetricVariableStore)
			store = (MetricVariableStore) variableStores.get(name);
		else
			store = new MetricVariableStore();
		
		Value value = compute(store, time);
		
		variableStores.put(name, store);
		
		return value;
	}
	
	public abstract Value compute(MetricVariableStore store, Instant time);
	
	public void updateStore(VariableStores variableStores, Metric metric) {
		MetricVariableStore store = null;
		if(variableStores.containsKey(name) && variableStores.get(name) instanceof MetricVariableStore)
			store = (MetricVariableStore) variableStores.get(name);
		else
			store = new MetricVariableStore();
		
		updateStore(store, metric);
		
		variableStores.put(name, store);
	}
	
	public abstract void updateStore(MetricVariableStore store, Metric metric);
	
	public static Optional<Class<? extends Value>> typeFromAggregation(Properties metricVariableProperties) throws ConfigurationException {
		String aggregateVal = metricVariableProperties.getProperty("aggregate");
		
		if(aggregateVal == null)
			return Optional.empty();
		
		aggregateVal = aggregateVal.toUpperCase();
		
		try {
			FloatMetricVariable.Operation.valueOf(aggregateVal);
			
			return Optional.of(FloatValue.class);
		}catch(IllegalArgumentException e) {}
		try {
			StringMetricVariable.Operation.valueOf(aggregateVal);
			
			return Optional.of(StringValue.class);
		}catch(IllegalArgumentException e) {}
		try {
			BooleanMetricVariable.Operation.valueOf(aggregateVal);
			
			return Optional.of(BooleanValue.class);
		}catch(IllegalArgumentException e) {}
		
		throw new ConfigurationException("Aggregation function " + aggregateVal + " does not exist.");
	}

	public static Optional<Class<? extends Value>> returnTypeFromAggregation(Properties metricVariableProperties) throws ConfigurationException{
		String aggregateVal = metricVariableProperties.getProperty("aggregate");
		
		if(aggregateVal == null)
			return Optional.empty();
		
		aggregateVal = aggregateVal.toUpperCase();
		
		try {
			ch.cern.spark.metrics.defined.equation.var.FloatMetricVariable.Operation aggreagation = FloatMetricVariable.Operation.valueOf(aggregateVal);
			
			return Optional.of(FloatMetricVariable.getReturnType(aggreagation));
		}catch(IllegalArgumentException e) {}
		try {
			ch.cern.spark.metrics.defined.equation.var.StringMetricVariable.Operation aggreagation = StringMetricVariable.Operation.valueOf(aggregateVal);
			
			return Optional.of(StringMetricVariable.getReturnType(aggreagation));
		}catch(IllegalArgumentException e) {}
		try {
			ch.cern.spark.metrics.defined.equation.var.BooleanMetricVariable.Operation aggreagation = BooleanMetricVariable.Operation.valueOf(aggregateVal);
			
			return Optional.of(BooleanMetricVariable.getReturnType(aggreagation));
		}catch(IllegalArgumentException e) {}
		
		throw new ConfigurationException("Aggregation function " + aggregateVal + " does not exist.");
	}
	
}
