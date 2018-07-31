package ch.cern.exdemon.metrics.defined;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import ch.cern.exdemon.Driver;
import ch.cern.exdemon.components.Component;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentType;
import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.equation.Equation;
import ch.cern.exdemon.metrics.defined.equation.var.MetricVariable;
import ch.cern.exdemon.metrics.defined.equation.var.Variable;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.exdemon.metrics.filter.MetricsFilter;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.utils.Pair;
import lombok.Getter;
import lombok.ToString;

@ToString
@ComponentType(Type.METRIC)
public final class DefinedMetric extends Component {

	private static final long serialVersionUID = 82179461944060520L;

	private Set<String> metricsGroupBy;
	
	@Getter
	private When when;
	
	@Getter
	private Equation equation;

	private ConfigurationException configurationException;
	
	private MetricsFilter filter;

    private Map<String, String> fixedValueAttributes;

    private Map<String, String> triggeringAttributes;

    public DefinedMetric() {
    }
    
	public DefinedMetric(String id) {
		setId(id);
	}

	@Override
	public ConfigurationResult config(Properties properties) {	
	    ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
	    
		String groupByVal = properties.getProperty("metrics.groupby");
		if(groupByVal != null)
			metricsGroupBy = Arrays.stream(groupByVal.split(" ")).map(String::trim).collect(Collectors.toSet());
		
		try {
            filter = MetricsFilter.build(properties.getSubset("metrics.filter"));
        } catch (ConfigurationException e) {
            confResult.withError("metrics.filter", e);
        }
		
		fixedValueAttributes = properties.getSubset("metrics.attribute").entrySet().stream()      //TODO || DEPRECATED
		                            .filter(entry -> entry.getKey().toString().endsWith(".fixed") || !entry.getKey().toString().contains("."))
		                            .map(entry -> new Pair<String, String>(entry.getKey().toString().replace(".fixed", ""), entry.getValue().toString()))
		                            .collect(Collectors.toMap(Pair::first, Pair::second));
		
		triggeringAttributes = properties.getSubset("metrics.attribute").entrySet().stream()
                                    .filter(entry -> entry.getKey().toString().endsWith(".triggering"))
                                    .map(entry -> new Pair<String, String>(entry.getKey().toString().replace(".triggering", ""), entry.getValue().toString()))
                                    .collect(Collectors.toMap(Pair::first, Pair::second));
		
		Properties variablesProperties = properties.getSubset("variables");
		Set<String> variableNames = variablesProperties.getIDs();
		
		String equationString = properties.getProperty("value");
		try {
			if(equationString == null && variableNames.size() == 1)	
				equation = new Equation(variableNames.iterator().next(), variablesProperties);
			else if(equationString == null)
			    return confResult.withMustBeConfigured("value");
			else
				equation = new Equation(equationString, variablesProperties);
		} catch (Exception e) {
			return confResult.withError("value", e);
		}
		
		Map<String, Variable> allVariables = new HashMap<>(equation.getVariables());
		//Parse variables that were not used in the equation
		for (String variableName : variableNames) {
		    if(allVariables.containsKey(variableName))
		        continue;
		    
            MetricVariable variable = new MetricVariable(variableName);
            ConfigurationResult varConfResult = variable.config(variablesProperties.getSubset(variableName), Optional.empty());
            confResult.merge("variables."+variableName, varConfResult);
            
            allVariables.put(variableName, variable);
        }
		
        try {
            Optional<Duration> batchDurationOpt = properties.getPeriod(Driver.BATCH_INTERVAL_PARAM);
            
            if(!batchDurationOpt.isPresent())
                throw new RuntimeException(Driver.BATCH_INTERVAL_PARAM + " must be configured");

            when = When.from(batchDurationOpt.get(), allVariables, properties.getProperty("when", "ANY"));
        } catch (ConfigurationException e) {
            confResult.withError(null, e);
        }
		
		return confResult.merge(null, properties.warningsIfNotAllPropertiesUsed());
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
			variableToUpdate.updateVariableStatuses(stores, metricForStore, metric.clone());
	}

	public Optional<Metric> generateByUpdate(VariableStatuses stores, Metric metric, Map<String, String> groupByAttributes) {
	    if(!filter.test(metric))
            return Optional.empty();
	    
		if(when.isTriggerBy(metric))
			return generate(stores, metric.getTimestamp(), groupByAttributes, Optional.of(metric));
		
		return Optional.empty();
	}
	
	public Optional<Metric> generateByBatch(VariableStatuses stores, Instant batchTime, Map<String, String> groupByAttributes) {
		if(when.isTriggerAt(batchTime))
			return generate(stores, batchTime, groupByAttributes, Optional.empty());
		
		return Optional.empty();
	}
	
	private Optional<Metric> generate(VariableStatuses stores, Instant time, Map<String, String> groupByAttributes, Optional<Metric> triggeringMetric) {		
		Map<String, String> metricAttributes = new HashMap<>(groupByAttributes);
		metricAttributes.put("$defined_metric", getId());
		metricAttributes.putAll(fixedValueAttributes);
		triggeringMetric.ifPresent(m -> {
		    triggeringAttributes.forEach((attribute, key) -> {
		        metricAttributes.put(attribute, m.getAttributes().get(key));
		    });
		});
		
		if(configurationException != null) {
			if(stores.newProcessedBatchTime(time))
				return Optional.of(new Metric(time, new ExceptionValue("ConfigurationException: " + configurationException.getMessage()), metricAttributes));
			else
				return Optional.empty();
		}
		
		Value value = equation.compute(stores, time);
			
		return Optional.of(new Metric(time, value, metricAttributes));
	}

	public Optional<Map<String, String>> getGroupByAttributes(Map<String, String> metricAttributes) {
		if(metricsGroupBy == null)
			return Optional.of(new HashMap<>());
		
		if(metricsGroupBy.contains("ALL"))
			return Optional.of(metricAttributes);
		
		Map<String, String> values = metricsGroupBy.stream()
			.map(id -> new Pair<String, String>(id, metricAttributes.get(id)))
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
