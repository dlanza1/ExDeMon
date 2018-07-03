package ch.cern.exdemon.metrics.defined.equation.var;

import java.time.Instant;
import java.util.Optional;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.value.PropertiesValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class PropertiesVariable extends Variable {

	private Properties properties;

	public PropertiesVariable(String name) {
		super(name);
	}
	
	@Override
	public Variable config(Properties properties, Optional<Class<? extends Value>> type) throws ConfigurationException {
		this.properties = properties;
		
		return this;
	}

	@Override
	public Value compute(VariableStatuses store, Instant time) {
		PropertiesValue value = new PropertiesValue(name, properties);
		value.setSource("props(" + name + ")");
		
		return value;
	}

	@Override
	public Class<? extends Value> returnType() {
		return PropertiesValue.class;
	}

	@Override
	public boolean test(Metric t) {
		return false;
	}
	
	@Override
	public String toString() {
		return "props(" + name + ")";
	}

}
