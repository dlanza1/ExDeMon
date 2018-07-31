package ch.cern.exdemon.metrics.defined.equation.var;

import java.util.Optional;
import java.util.function.Predicate;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.value.PropertiesValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusValue;

public abstract class Variable implements ValueComputable, Predicate<Metric> {
	
	protected String name;

	public Variable(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}

	public ConfigurationResult config(Properties properties, Optional<Class<? extends Value>> typeOpt) {
		return ConfigurationResult.SUCCESSFUL();
	}
	
	public abstract StatusValue updateStatus(Optional<StatusValue> statusOpt, Metric metric, Metric originalMetric);

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
