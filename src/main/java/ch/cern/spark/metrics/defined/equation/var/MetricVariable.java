package ch.cern.spark.metrics.defined.equation.var;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

	private Set<String> aggregateSelectAtt;

	public MetricVariable(String name) {
		super(name);
	}

	public MetricVariable config(Properties properties) throws ConfigurationException {
		filter = MetricsFilter.build(properties.getSubset("filter"));
		
		if(properties.containsKey("expire") && properties.getProperty("expire").toLowerCase().equals("never"))
			expirePeriod = null;
		else
			expirePeriod = properties.getPeriod("expire", Duration.ofMinutes(10));
		
		String aggregateSelect = properties.getProperty("aggregate.attributes", "ALL");
		if(aggregateSelect == null || aggregateSelect.equals("ALL"))
			aggregateSelectAtt = null;
		else
			aggregateSelectAtt = new HashSet<String>(Arrays.asList(aggregateSelect.split("\\s")));
		
		return this;
	}

	@Override
	public boolean test(Metric metric) {
		return filter.test(metric);
	}

	@Override
	public Value compute(VariableStatuses variableStores, Instant time) {
		MetricVariableStatus store = null;
		if(variableStores.containsVariable(name) && variableStores.get(name) instanceof MetricVariableStatus)
			store = (MetricVariableStatus) variableStores.get(name);
		else
			store = new MetricVariableStatus();
		
		Value value = compute(store, time);
		
		variableStores.put(name, store);
		
		return value;
	}
	
	public abstract Value compute(MetricVariableStatus store, Instant time);
	
	public void updateStore(VariableStatuses variableStores, Metric metric) {
		MetricVariableStatus store = null;
		if(variableStores.containsVariable(name) && variableStores.get(name) instanceof MetricVariableStatus)
			store = (MetricVariableStatus) variableStores.get(name);
		else
			store = new MetricVariableStatus();
		
		metric.setIDs(getAggSelectAttributes(metric.getIDs()));
		
		updateStore(store, metric);
		
		variableStores.put(name, store);
	}

	public abstract void updateStore(MetricVariableStatus store, Metric metric);
	
	private Map<String, String> getAggSelectAttributes(Map<String, String> attributes) {
		if(aggregateSelectAtt == null)
			return attributes;
		
		return attributes.entrySet().stream()
					.filter(entry -> aggregateSelectAtt.contains(entry.getKey()))
					.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}
	
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
