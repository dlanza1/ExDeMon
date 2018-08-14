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
import ch.cern.utils.Pair;

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
        for (String mergeVariablesName : mergeVariablesNames) {
            Variable mergeVariable = variables.get(mergeVariablesName);
            if(mergeVariable == null) {
                try {
                    mergeVariable = Variable.create(mergeVariablesName, variablesProperties, Optional.empty(), variables);
                } catch (ConfigurationException e) {
                    confResult.withError("merge.variables", e);
                }
            }
            
            if(mergeVariable == null)
                confResult.withError("merge.variables", "variable with name \""+mergeVariablesName+"\" does not exist");
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
	    return new VariableStatus();
	}

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

    public static Variable create(String name, Properties variablesProperties, Optional<Class<? extends Value>> argumentTypeOpt, Map<String, Variable> variables) throws ConfigurationException {
        Variable var = null;
        
        Properties properties = variablesProperties.getSubset(name);
        
        if(argumentTypeOpt.isPresent() && argumentTypeOpt.get().equals(PropertiesValue.class)) {
            var = new PropertiesVariable(name, variables, variablesProperties);
        }else if(properties.containsKey("attribute")){
            var = new AttributeVariable(name, variables, variablesProperties);
        }else if(properties.containsKey("fixed.value")){
            var = new FixedValueVariable(name, variables, variablesProperties);
        }else {
            var = new ValueVariable(name, variables, variablesProperties);
        }
        
        ConfigurationResult configResult = var.config(properties, argumentTypeOpt);
        
        if(!configResult.getErrors().isEmpty())
            throw new ConfigurationException(name, configResult.getErrors().toString());
        
        return var;
    }
	
}
