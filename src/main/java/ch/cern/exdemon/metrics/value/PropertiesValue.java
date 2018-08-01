package ch.cern.exdemon.metrics.value;

import java.time.Instant;
import java.util.Optional;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.properties.Properties;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

@EqualsAndHashCode(callSuper=false)
public class PropertiesValue extends Value implements ValueComputable{

	private static final long serialVersionUID = 3844172330757307935L;
	
	private String properties_name;
	
	private Properties props;

	public PropertiesValue(String name, @NonNull Properties props) {
		this.properties_name = name;
		this.props = props;
	}
	
	@Override
	public Optional<Properties> getAsProperties() {
		return Optional.of(props);
	}

	@Override
	public Value compute(VariableStatuses store, Instant time) {
		return this;
	}

	@Override
	public Class<? extends Value> returnType() {
		return PropertiesValue.class;
	}
	
	@Override
	public String toString() {
		return "props(" + properties_name + ")";
	}
	
	public String getName() {
		return properties_name;
	}

}
