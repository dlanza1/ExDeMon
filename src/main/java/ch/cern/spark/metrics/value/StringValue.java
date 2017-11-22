package ch.cern.spark.metrics.value;

import java.time.Instant;
import java.util.Optional;

import ch.cern.spark.metrics.defined.equation.ValueComputable;
import ch.cern.spark.metrics.defined.equation.var.VariableStores;

public class StringValue extends Value implements ValueComputable{

	private static final long serialVersionUID = 6026199196915653369L;

	private String str;
	
	public StringValue(String value){
		this.str = value;
	}

	@Override
	public Optional<String> getAsString() {
		return Optional.of(str);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((str == null) ? 0 : str.hashCode());
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
		StringValue other = (StringValue) obj;
		if (str == null) {
			if (other.str != null)
				return false;
		} else if (!str.equals(other.str))
			return false;
		return true;
	}

	@Override
	public StringValue compute(VariableStores store, Instant time) {
		return new StringValue(str);
	}
	
	@Override
	public Class<StringValue> returnType() {
		return StringValue.class;
	}
	
	@Override
	public String toString() {
		return "\"" + str + "\"";
	}
	
	@Override
	public String getSource() {
		if(source == null)
			return toString();
		else
			return super.toString();
	}

}
