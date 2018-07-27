package ch.cern.exdemon.metrics.defined.equation.var;

import java.util.Optional;
import java.util.function.Predicate;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public abstract class Variable implements ValueComputable, Predicate<Metric> {
	
	protected String name;

	public Variable(String name) {
		this.name = name;
	}
	
	public String getName() {
		return name;
	}

	public ConfigurationResult config(Properties properties, Optional<Class<? extends Value>> typeOpt) throws ConfigurationException {
		return ConfigurationResult.SUCCESSFUL();
	}
	
}
