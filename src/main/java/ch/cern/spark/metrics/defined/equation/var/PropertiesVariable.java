package ch.cern.spark.metrics.defined.equation.var;

import java.time.Instant;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.value.PropertiesValue;
import ch.cern.spark.metrics.value.Value;

public class PropertiesVariable extends Variable {

	private Properties properties;

	public PropertiesVariable(String name) {
		super(name);
	}
	
	@Override
	public Variable config(Properties properties) throws ConfigurationException {
		this.properties = properties;
		
		return this;
	}

	@Override
	public Value compute(VariableStores store, Instant time) {
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
