package ch.cern.spark.metrics.defined;

import java.io.Serializable;
import java.text.ParseException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.equation.Equation;
import ch.cern.spark.metrics.defined.equation.var.MetricVariable;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.metrics.filter.MetricsFilter;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.Value;
import ch.cern.utils.Pair;
import lombok.Getter;
import lombok.ToString;

@ToString
public class DefinedMetric implements Serializable{

	private static final long serialVersionUID = 82179461944060520L;
	
	private final static Logger LOG = Logger.getLogger(DefinedMetric.class.getName());

	@Getter
	private String id;

	private Set<String> metricsGroupBy;
	
	@Getter
	private Set<String> variablesWhen;
	
	@Getter
	private Equation equation;

	private ConfigurationException configurationException;
	
	private MetricsFilter filter;

    private Map<String, String> fixedValueAttributes;

	public DefinedMetric(String id) {
		this.id = id;
	}

	public DefinedMetric config(Properties properties) {
		try {
			return tryConfig(properties);
		} catch (ConfigurationException e) {
		    LOG.error(id + ": " + e.getMessage(), e);
		    
			configurationException = e;
			
			variablesWhen = null; // So that, it is trigger with every batch
			metricsGroupBy = null;
			equation = null;
			
			return this;
		}
	}
	
	public DefinedMetric tryConfig(Properties properties) throws ConfigurationException {		
		String groupByVal = properties.getProperty("metrics.groupby");
		if(groupByVal != null)
			metricsGroupBy = Arrays.stream(groupByVal.split(" ")).map(String::trim).collect(Collectors.toSet());
		
		filter = MetricsFilter.build(properties.getSubset("metrics.filter"));
		
		fixedValueAttributes = properties.getSubset("metrics.attribute").entrySet().stream()
		                            .map(entry -> new Pair<String, String>(entry.getKey().toString(), entry.getValue().toString()))
		                            .collect(Collectors.toMap(Pair::first, Pair::second));
		
		Properties variablesProperties = properties.getSubset("variables");
		Set<String> variableNames = variablesProperties.getIDs();
		
		String equationString = properties.getProperty("value");
		try {
			if(equationString == null && variableNames.size() == 1)	
				equation = new Equation(variableNames.iterator().next(), variablesProperties);
			else if(equationString == null)
				throw new ConfigurationException("Value must be specified.");
			else
				equation = new Equation(equationString, variablesProperties);
		} catch (ParseException e) {
			throw new ConfigurationException("Problem parsing value: " + e.getMessage());
		}
		
		variablesWhen = new HashSet<String>();
		String whenValue = properties.getProperty("when", "ANY");
		if(whenValue != null && whenValue.toUpperCase().equals("ANY"))
			variablesWhen.addAll(variableNames);
		else if(whenValue != null && whenValue.toUpperCase().equals("BATCH"))
			variablesWhen = null;
		else if(whenValue != null) {			
			variablesWhen.addAll(Arrays.stream(whenValue.split(" ")).map(String::trim).collect(Collectors.toSet()));
		}
		if(variablesWhen != null) {
			for (String variableWhen : variablesWhen) {
				if(!equation.getVariables().containsKey(variableWhen)) {
					MetricVariable trigger = new MetricVariable(variableWhen);
					trigger.config(variablesProperties.getSubset(variableWhen), Optional.empty());
					
					equation.getVariables().put(variableWhen, trigger);
				}
			
				if(!variableNames.contains(variableWhen))
					throw new ConfigurationException("Variables listed in when parameter must be declared.");
			}
		}
		if(whenValue == null) {
			variablesWhen.add(equation.getMetricVariables().keySet().stream().sorted().findFirst().get());
		}
		
		variablesProperties.confirmAllPropertiesUsed();
		properties.confirmAllPropertiesUsed();
		
		configurationException = null;
		
		return this;
	}

	public boolean testIfApplyForAnyVariable(Metric metric) {
		if(configurationException != null)
			return true;
		
		if(!filter.test(metric))
			return false;
		
		return equation.getVariables().values().stream()
				.filter(variable -> variable.test(metric))
				.count() > 0;
	}

	public Map<String, MetricVariable> getVariablesToUpdate(Metric metric) {
		return getMetricVariables().entrySet().stream()
				.filter(entry -> entry.getValue().test(metric))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	public boolean shouldBeTrigeredByUpdate(Metric metric) {
		if(isTriggerOnEveryBatch())
			return false;
		
		if(!filter.test(metric))
			return false;
		
		return equation.getVariables().entrySet().stream()
				.filter(entry -> variablesWhen.contains(entry.getKey()))
				.map(entry -> entry.getValue())
				.filter(variable -> variable.test(metric))
				.count() > 0;
	}
	
	private boolean isTriggerOnEveryBatch() {
		return variablesWhen == null;
	}

	public void updateStore(VariableStatuses stores, Metric metric, Set<String> groupByKeys) throws CloneNotSupportedException {
		if(configurationException != null)
			return;
		
		if(!filter.test(metric))
			return;
		
		Map<String, MetricVariable> variablesToUpdate = getVariablesToUpdate(metric);
		
		Metric metricForStore = metric.clone();
		if(groupByKeys != null)
			metricForStore.getAttributes().entrySet().removeIf(entry -> groupByKeys.contains(entry.getKey()));
		
		for (MetricVariable variableToUpdate : variablesToUpdate.values())
			variableToUpdate.updateVariableStatuses(stores, metricForStore);
	}

	public Optional<Metric> generateByUpdate(VariableStatuses stores, Metric metric, Map<String, String> groupByMetricIDs) {
		if(!shouldBeTrigeredByUpdate(metric))
			return Optional.empty();
		
		return generate(stores, metric.getTimestamp(), groupByMetricIDs);
	}
	
	public Optional<Metric> generateByBatch(VariableStatuses stores, Instant time, Map<String, String> groupByMetricIDs) {
		if(!isTriggerOnEveryBatch())
			return Optional.empty();
		
		return generate(stores, time, groupByMetricIDs);
	}
	
	private Optional<Metric> generate(VariableStatuses stores, Instant time, Map<String, String> groupByMetricIDs) {		
		Map<String, String> metricIDs = new HashMap<>(groupByMetricIDs);
		metricIDs.put("$defined_metric", id);
		metricIDs.putAll(fixedValueAttributes);
		
		if(configurationException != null) {
			if(stores.newProcessedBatchTime(time))
				return Optional.of(new Metric(time, new ExceptionValue("ConfigurationException: " + configurationException.getMessage()), metricIDs));
			else
				return Optional.empty();
		}
		
		Value value = equation.compute(stores, time);
			
		return Optional.of(new Metric(time, value, metricIDs));
	}

	public Optional<Map<String, String>> getGroupByMetricIDs(Map<String, String> metricIDs) {
		if(metricsGroupBy == null)
			return Optional.of(new HashMap<>());
		
		if(metricsGroupBy.contains("ALL"))
			return Optional.of(metricIDs);
		
		Map<String, String> values = metricsGroupBy.stream()
			.map(id -> new Pair<String, String>(id, metricIDs.get(id)))
			.filter(pair -> pair.second() != null)
			.collect(Collectors.toMap(Pair::first, Pair::second));
		
		return values.size() == metricsGroupBy.size() ? Optional.of(values) : Optional.empty();
	}
	
	protected Map<String, MetricVariable> getMetricVariables() {
		return equation.getVariables().entrySet().stream()
						.filter(entry -> entry.getValue() instanceof MetricVariable)
						.map(entry -> new Pair<String, MetricVariable>(entry.getKey(), (MetricVariable) entry.getValue()))
						.collect(Collectors.toMap(Pair::first, Pair::second));
	}

}
