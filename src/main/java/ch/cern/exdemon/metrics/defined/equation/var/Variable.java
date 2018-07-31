package ch.cern.exdemon.metrics.defined.equation.var;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.function.Predicate;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.value.PropertiesValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public abstract class Variable implements ValueComputable, Predicate<Metric> {
	
	protected String name;
	
    private Duration delay;

	public Variable(String name) {
		this.name = name;
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
		
        return confResult;
	}
	
	public VariableStatus updateStatus(Optional<VariableStatus> statusOpt, Metric metric, Metric originalMetric) {
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
	public Value compute(VariableStatuses stores, Instant time) {
	    VariableStatus store = null;
	    if(stores != null)
	        store = stores.get(getName());
	    
        return compute(Optional.ofNullable(store ), time);
	}

    protected abstract Value compute(Optional<VariableStatus> statusValue, Instant time);

    public static Variable create(String name, Properties properties, Optional<Class<? extends Value>> argumentTypeOpt) throws ConfigurationException {
        Variable var = null;
        
        if(argumentTypeOpt.isPresent() && argumentTypeOpt.get().equals(PropertiesValue.class)) {
            var = new PropertiesVariable(name);
        }else if(properties.containsKey("attribute")){
            var = new AttributeVariable(name);
        }else if(properties.containsKey("fixed.value")){
            var = new FixedValueVariable(name);
        }else {
            var = new ValueVariable(name);
        }
        
        ConfigurationResult configResult = var.config(properties, argumentTypeOpt);
        
        if(!configResult.getErrors().isEmpty())
            throw new ConfigurationException(name, configResult.getErrors().toString());
        
        return var;
    }
	
}
