package ch.cern.exdemon.metrics.defined;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
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
import ch.cern.exdemon.metrics.defined.equation.var.Variable;
import ch.cern.exdemon.metrics.defined.equation.var.VariableCreationResult;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatus;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.exdemon.metrics.filter.MetricsFilter;
import ch.cern.exdemon.metrics.value.StringValue;
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
	
	private HashMap<String, Variable> variables;
	
	private MetricsFilter filter;

    private Map<String, String> fixedValueAttributes;
    private Map<String, String> triggeringAttributes;
    private Map<String, String> variableAttributes;

    private HashSet<String> lastSourceMetricsVariables;

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
		
		filter = new MetricsFilter();
		confResult.merge("metrics.filter", filter.config(properties.getSubset("metrics.filter")));
		
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
        } catch (ConfigurationException e) {
            return confResult.withError("value", e);
		} catch (Exception e) {
			return confResult.withError("value", e);
		}
		
		variables = new HashMap<>(equation.getVariables());
		//Parse variables that were not used in the equation
		for (String variableName : variableNames) {
		    if(variables.containsKey(variableName))
		        continue;
		    
            VariableCreationResult variableCreatioinResult = Variable.create(variableName, variablesProperties, Optional.empty(), variables);
            variableCreatioinResult.getVariable().ifPresent(var -> variables.put(variableName, var));
            
            confResult.merge("variables."+variableName, variableCreatioinResult.getConfigResult());
        }
		
		if(properties.containsKey("metrics.last_source_metrics.variables")) {
		    lastSourceMetricsVariables = new HashSet<>(Arrays.asList(properties.getProperty("metrics.last_source_metrics.variables").split("\\s")));
		    
		    lastSourceMetricsVariables.forEach(var -> {
		        if(!variables.containsKey(var))
	                confResult.withError("metrics.last_source_metrics.variables", "variable with name \""+var+"\" does not exist");
		    });
		}else {
		    lastSourceMetricsVariables = null;
		}
	      
        fixedValueAttributes = properties.getSubset("metrics.attribute").entrySet().stream()      //TODO || DEPRECATED
                                    .filter(entry -> entry.getKey().toString().endsWith(".fixed") || !entry.getKey().toString().contains("."))
                                    .map(entry -> new Pair<String, String>(entry.getKey().toString().replace(".fixed", ""), entry.getValue().toString()))
                                    .collect(Collectors.toMap(Pair::first, Pair::second));
        
        triggeringAttributes = properties.getSubset("metrics.attribute").entrySet().stream()
                                    .filter(entry -> entry.getKey().toString().endsWith(".triggering"))
                                    .map(entry -> new Pair<String, String>(entry.getKey().toString().replace(".triggering", ""), entry.getValue().toString()))
                                    .collect(Collectors.toMap(Pair::first, Pair::second));
        
        variableAttributes = properties.getSubset("metrics.attribute").entrySet().stream()
                                    .filter(entry -> entry.getKey().toString().endsWith(".variable"))
                                    .map(entry -> new Pair<String, String>(entry.getKey().toString().replace(".variable", ""), entry.getValue().toString()))
                                    .collect(Collectors.toMap(Pair::first, Pair::second));
        variableAttributes.forEach((attribute, variable) -> {
            if(!variables.containsKey(variable))
                confResult.withError("metrics.attribute."+attribute+".variable", "variable with name \""+variable+"\" does not exist");
            
            if(!variables.get(variable).returnType().equals(StringValue.class))
                confResult.withError("metrics.attribute."+attribute+".variable", "variable \""+variable+"\" does not return string type");
        });
		
        try {
            Optional<Duration> batchDurationOpt = properties.getPeriod(Driver.BATCH_INTERVAL_PARAM);
            
            if(!batchDurationOpt.isPresent())
                throw new RuntimeException(Driver.BATCH_INTERVAL_PARAM + " must be configured");

            when = When.from(batchDurationOpt.get(), variables, properties.getProperty("when", "ANY"));
        } catch (ConfigurationException e) {
            confResult.withError(null, e);
        }
		
		return confResult.merge(null, properties.warningsIfNotAllPropertiesUsed());
	}

	public boolean testIfApplyForAnyVariable(Metric metric) {
		if(!filter.test(metric))
			return false;
		
		return variables.values().stream()
				.filter(variable -> variable.test(metric))
				.count() > 0;
	}

	public Map<String, Variable> getVariablesToUpdate(Metric metric) {
		return variables.entrySet().stream()
				.filter(entry -> entry.getValue().test(metric))
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
	}

	public void updateStore(VariableStatuses stores, Metric metric, Set<String> groupByKeys) {
		if(!filter.test(metric))
			return;
		
		Map<String, Variable> variablesToUpdate = getVariablesToUpdate(metric);
		
		Metric metricForStore = metric.clone();
		if(groupByKeys != null)
			metricForStore.getAttributes().entrySet().removeIf(entry -> groupByKeys.contains(entry.getKey()));
		
		for (Variable variableToUpdate : variablesToUpdate.values()) {
		    String name = variableToUpdate.getName();
		    
		    Optional<VariableStatus> status = Optional.ofNullable(stores.get(name))
		                                           .filter(s -> s instanceof VariableStatus)
		                                           .map(s -> (VariableStatus) s);
		    
		    VariableStatus updatedStatus = variableToUpdate.updateStatus(status, metricForStore, metric.clone());
			
		    stores.put(name, updatedStatus);
		}
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
		Map<String, String> attributes = new HashMap<>(groupByAttributes);
		attributes.put("$defined_metric", getId());
		attributes.putAll(fixedValueAttributes);
		triggeringMetric.ifPresent(m -> {
		    triggeringAttributes.forEach((attribute, key) -> {
		        attributes.put(attribute, m.getAttributes().get(key));
		    });
		});
		variableAttributes.forEach((attribute, variable) -> {
		    Value value = variables.get(variable).compute(stores, time);
		    
		    if(value != null)
    		    value.getAsString().ifPresent(str -> {
    		        attributes.put(attribute, str); 
    		    });		    
		});
		
		Value value = equation.compute(stores, time);
		
		if(lastSourceMetricsVariables != null) {
		    List<Metric> lastSourceMetrics = lastSourceMetricsVariables.stream().map(varName -> variables.get(varName))
                                            		                                      .map(var -> var.compute(stores, time))
                                            		                                      .map(val -> val.getLastSourceMetrics())
                                            		                                      .filter(metrics -> metrics != null)
                                            		                                      .flatMap(List::stream)
                                                                                          .distinct()
                                                                                          .collect(Collectors.toList());
	        if(lastSourceMetrics.isEmpty())
	            value.setLastSourceMetrics(null);
	        else
	            value.setLastSourceMetrics(lastSourceMetrics);
		}
			
		return Optional.of(new Metric(time, value, attributes));
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

}
