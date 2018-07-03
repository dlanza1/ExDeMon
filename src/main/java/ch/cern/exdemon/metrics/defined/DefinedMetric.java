package ch.cern.exdemon.metrics.defined;

import java.text.ParseException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import ch.cern.exdemon.components.Component;
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
public final class DefinedMetric extends Component {

	private static final long serialVersionUID = 82179461944060520L;
	
	private final static Logger LOG = Logger.getLogger(DefinedMetric.class.getName());

	private Set<String> metricsGroupBy;
	
	@Getter
	private When when;
	
	@Getter
	private Equation equation;

	private ConfigurationException configurationException;
	
	private MetricsFilter filter;

    private Map<String, String> fixedValueAttributes;

    public DefinedMetric() {
    }
    
	public DefinedMetric(String id) {
		setId(id);
	}

	@Override
	public void config(Properties properties) {
		try {
			tryConfig(properties);
		} catch (ConfigurationException e) {
		    LOG.error(getId() + ": " + e.getMessage(), e);
		    
			configurationException = e;
			
			try {
                when = When.from("1m");
            } catch (ConfigurationException e1) {}
			metricsGroupBy = null;
			equation = null;
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
		
		Map<String, Variable> allVariables = new HashMap<>(equation.getVariables());
		//Parse variables that were not used in the equation
		for (String variableName : variableNames) {
		    if(allVariables.containsKey(variableName))
		        continue;
		    
            MetricVariable variable = new MetricVariable(variableName);
            variable.config(variablesProperties.getSubset(variableName), Optional.empty());
            
            allVariables.put(variableName, variable);
        }
		
		when = When.from(allVariables, properties.getProperty("when", "ANY"));		
		
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

	public Optional<Metric> generateByUpdate(VariableStatuses stores, Metric metric, Map<String, String> groupByMetricIDs) {
	    if(!filter.test(metric))
            return Optional.empty();
	    
		if(when.isTriggerBy(metric))
			return generate(stores, metric.getTimestamp(), groupByMetricIDs);
		
		return Optional.empty();
	}
	
	public Optional<Metric> generateByBatch(VariableStatuses stores, Instant batchTime, Map<String, String> groupByMetricIDs) {
		if(when.isTriggerAt(batchTime))
			return generate(stores, batchTime, groupByMetricIDs);
		
		return Optional.empty();
	}
	
	private Optional<Metric> generate(VariableStatuses stores, Instant time, Map<String, String> groupByMetricIDs) {		
		Map<String, String> metricIDs = new HashMap<>(groupByMetricIDs);
		metricIDs.put("$defined_metric", getId());
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
