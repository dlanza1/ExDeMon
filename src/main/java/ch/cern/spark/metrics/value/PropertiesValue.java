package ch.cern.spark.metrics.value;

import java.time.Instant;
import java.util.Optional;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.defined.equation.ValueComputable;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;

public class PropertiesValue extends Value implements ValueComputable{

	private static final long serialVersionUID = 3844172330757307935L;
	
	private String properties_name;
	
	private Properties props;

	public PropertiesValue(String name, Properties props) {
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

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((properties_name == null) ? 0 : properties_name.hashCode());
        result = prime * result + ((props == null) ? 0 : props.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        PropertiesValue other = (PropertiesValue) obj;
        if (properties_name == null) {
            if (other.properties_name != null)
                return false;
        } else if (!properties_name.equals(other.properties_name))
            return false;
        if (props == null) {
            if (other.props != null)
                return false;
        } else if (!props.equals(other.props))
            return false;
        return true;
    }
	
}
