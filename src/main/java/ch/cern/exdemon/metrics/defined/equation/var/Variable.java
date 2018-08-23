package ch.cern.exdemon.metrics.defined.equation.var;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.value.PropertiesValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.storage.ClassNameAlias;
import ch.cern.utils.Pair;
import lombok.EqualsAndHashCode;
import lombok.ToString;

public abstract class Variable implements ValueComputable, Predicate<Metric> {
	
	protected String name;
	
    private Duration delay;

    private Map<String, Variable> variables;
    private Properties variablesProperties;
    
    private List<Variable> resultFromVariables;

	protected Variable(String name, Map<String, Variable> variables, Properties variablesProperties) {
		this.name = name;
		this.variables = variables;
		this.variablesProperties = variablesProperties;
		
		variables.put(name, this);
	}
	
	public String getName() {
		return name;
	}

	public ConfigurationResult config(Properties properties, Optional<Class<? extends Value>> typeOpt) {
		ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
		
		try {
            delay = properties.getPeriod("timestamp.shift", Duration.ZERO);
        } catch (ConfigurationException e) {
            confResult.withError(null, e);
        }
		
		configMergeVariables(properties, confResult);
		
        return confResult;
	}
	
	private void configMergeVariables(Properties properties, ConfigurationResult confResult) {
        String mergeVariablesStr = properties.getProperty("merge.variables", "");
        
        List<String> mergeVariablesNames = Arrays.stream(mergeVariablesStr.split(" "))
                                                 .filter(name -> !name.isEmpty())
                                                 .collect(Collectors.toList());
        
        resultFromVariables = new LinkedList<>();
        resultFromVariables.add(this);
        for (String mergeVariableName : mergeVariablesNames) {
            Variable mergeVariable = variables.get(mergeVariableName);
            if(mergeVariable == null) {
                VariableCreationResult variableCreationResult = Variable.create(mergeVariableName, variablesProperties, Optional.empty(), variables);
                if(variableCreationResult.getVariable().isPresent())
                    mergeVariable = variableCreationResult.getVariable().get();
                
                confResult.merge("variables."+mergeVariableName, variableCreationResult.getConfigResult());
            }
            
            if(mergeVariable == null)
                confResult.withError("merge.variables", "variable with name \""+mergeVariableName+"\" does not exist");
            else
                resultFromVariables.add(mergeVariable);
        }
    }

    public final VariableStatus updateStatus(Optional<VariableStatus> statusOpt, Metric metric, Metric originalMetric) {
	    VariableStatus status = statusOpt.orElse(initStatus());

	    metric.setTimestamp(metric.getTimestamp().plus(delay));
	    
	    status.setLastUpdateMetricTime(metric.getTimestamp());
	    
        return updateStatus(status, metric, originalMetric);
	}

	protected VariableStatus updateStatus(VariableStatus status, Metric metric, Metric originalMetric) {
	    return status;
	}

	protected VariableStatus initStatus() {
	    return new Status_();
	};

    @Override
	public final Value compute(VariableStatuses stores, Instant time) {
        Variable variableToCompute = resultFromVariables.stream()
                                                        .filter(var -> var.delay.equals(Duration.ZERO) || getLastUpdateMetricTime(stores, var).compareTo(time) <= 0)
                                                        .map(var -> new Pair<String, Instant>(var.name, getLastUpdateMetricTime(stores, var)))
                                                        .max((a, b) -> a.second.compareTo(b.second))
                                                        .map(pair -> variables.get(pair.first))
                                                        .orElse(this);
        
        Optional<VariableStatus> status = getStatus(stores, variableToCompute.name);
        
        return variableToCompute.compute(status, time);
	}

    private Instant getLastUpdateMetricTime(VariableStatuses stores, Variable var) {
        Optional<VariableStatus> statusOpt = getStatus(stores, var.name);

        return statusOpt.map(s -> s.getLastUpdateMetricTime()).orElse(var.getLastUpdateMetricTimeWhenNoStatus());
    }

    protected Instant getLastUpdateMetricTimeWhenNoStatus() {
        return Instant.EPOCH;
    }

    private Optional<VariableStatus> getStatus(VariableStatuses stores, String name) {
        if(stores == null)
            return Optional.empty();
        
        return Optional.ofNullable(stores.get(name));
    }

    protected abstract Value compute(Optional<VariableStatus> statusValue, Instant time);

    public static VariableCreationResult create(String name, Properties variablesProperties, Optional<Class<? extends Value>> argumentTypeOpt, Map<String, Variable> variables) {
        Properties properties = variablesProperties.getSubset(name);
        
        Variable var = null;
        
        if(argumentTypeOpt.isPresent() && argumentTypeOpt.get().equals(PropertiesValue.class)) {
            var = new PropertiesVariable(name, variables, variablesProperties);
        }else if(properties.containsKey("attribute")){
            var = new AttributeVariable(name, variables, variablesProperties);
        }else if(properties.containsKey("fixed.value")){
            var = new FixedValueVariable(name, variables, variablesProperties);
        }else {
            var = new ValueVariable(name, variables, variablesProperties);
        }
        
        return new VariableCreationResult(var, var.config(properties, argumentTypeOpt));
    }
    
    @ClassNameAlias("default-attribute-status")
    @EqualsAndHashCode(callSuper=true)
    @ToString
    public static class Status_ extends VariableStatus{
        private static final long serialVersionUID = -6219307165345965744L;
    }
	
}
