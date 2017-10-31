package ch.cern.spark.metrics.defined;

import java.io.Serializable;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.Pair;
import ch.cern.spark.metrics.Metric;

public class DefinedMetric implements Serializable{

	private static final long serialVersionUID = 82179461944060520L;

	private String name;
	
	private Map<String, Variable> variables;
	
	private Set<String> metricsGroupBy;
	
	private Set<String> variablesWhen;
	
	private Equation equation;

	public DefinedMetric(String name) {
		this.name = name;
	}

	public DefinedMetric config(Properties properties) throws ConfigurationException {		
		String groupByVal = properties.getProperty("metrics.groupby");
		if(groupByVal != null)
			metricsGroupBy = Arrays.stream(groupByVal.split(",")).map(String::trim).collect(Collectors.toSet());
		
		Properties variablesProperties = properties.getSubset("variables");
		Set<String> variableNames = variablesProperties.getUniqueKeyFields();
		variables = new HashMap<>();
		for (String variableName : variableNames)
			variables.put(variableName, new Variable(variableName).config(variablesProperties.getSubset(variableName)));
		if(variables.isEmpty())
			throw new ConfigurationException("At least a variable must be described.");
		
		String equationString = properties.getProperty("value");
		if(equationString == null && variables.size() == 1)
			equation = new Equation(variables.keySet().stream().findAny().get());
		else if(equationString == null)
			throw new ConfigurationException("Value must be specified.");
		else
			equation = new Equation(equationString);
		
		// Equation should be able to compute the result with all variables
		Map<String, Double> valuesTest = variables.keySet().stream()
			.map(name -> new Pair<String, Double>(name, Math.random()))
			.collect(Collectors.toMap(Pair::first, Pair::second));
		Optional<Double> resultTest = equation.compute(valuesTest);
		if(!resultTest.isPresent())
			throw new ConfigurationException("Equation (value) contain variables that have not been described.");
		
		variablesWhen = new HashSet<String>();
		String whenValue = properties.getProperty("when");
		if(whenValue != null && whenValue.toUpperCase().equals("ANY"))
			variablesWhen.addAll(variables.keySet());
		else if(whenValue != null && whenValue.toUpperCase().equals("BATCH"))
			variablesWhen = null;
		else if(whenValue != null)
			variablesWhen.addAll(Arrays.stream(whenValue.split(",")).map(String::trim).collect(Collectors.toSet()));
		else
			variablesWhen.add(variableNames.stream().sorted().findFirst().get());
		
		return this;
	}

	public boolean testIfApplyForAnyVariable(Metric metric) {
		return variables.values().stream().filter(variable -> variable.test(metric)).count() > 0;
	}

	public Map<String, Variable> getVariablesToUpdate(Metric metric) {
		return variables.entrySet().stream()
				.filter(entry -> entry.getValue().test(metric))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	public boolean shouldBeTrigeredByUpdate(Metric metric) {
		if(isTriggerOnEveryBatch())
			return false;
		
		return variables.entrySet().stream()
				.filter(entry -> variablesWhen.contains(entry.getKey()))
				.map(entry -> entry.getValue())
				.filter(variable -> variable.test(metric))
				.count() > 0;
	}
	
	private boolean isTriggerOnEveryBatch() {
		return variablesWhen == null;
	}

	public void updateStore(DefinedMetricStore store, Metric metric) {
		Map<String, Variable> variablesToUpdate = getVariablesToUpdate(metric);
		
		for (Variable variableToUpdate : variablesToUpdate.values())
			variableToUpdate.updateStore(store, metric);
	}

	public Optional<Metric> generateByUpdate(DefinedMetricStore store, Metric metric, Map<String, String> groupByMetricIDs) {
		if(!shouldBeTrigeredByUpdate(metric))
			return Optional.empty();
		
		return generate(store, metric.getInstant(), groupByMetricIDs);
	}
	
	public Optional<Metric> generateByBatch(DefinedMetricStore store, Instant metricTime, Map<String, String> groupByMetricIDs) {
		if(!isTriggerOnEveryBatch())
			return Optional.empty();
		
		return generate(store, metricTime, groupByMetricIDs);
	}
	
	private Optional<Metric> generate(DefinedMetricStore store, Instant metricTime, Map<String, String> groupByMetricIDs) {
		Map<String, Double> variableValues = new HashMap<>();
		for (Variable var : variables.values()) {
			Optional<Double> valueOpt = var.compute(store, metricTime);
			
			valueOpt.ifPresent(value -> variableValues.put(var.getName(), value));
		}
		
		Optional<Double> value = equation.compute(variableValues);
		
		if(value.isPresent()) {
			Map<String, String> metricIDs = new HashMap<>(groupByMetricIDs);
			metricIDs.put("$defined_metric", name);
			
			return Optional.of(new Metric(metricTime, value.get().floatValue(), metricIDs ));
		}else {
			return Optional.empty();
		}
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
	
	public String getName() {
		return name;
	}

	public Equation getEquation() {
		return equation;
	}

	protected Map<String, Variable> getVariables() {
		return variables;
	}
	
	protected Set<String> getVariablesWhen() {
		return variablesWhen;
	}

}
