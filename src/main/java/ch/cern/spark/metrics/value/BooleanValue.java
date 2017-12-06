package ch.cern.spark.metrics.value;

import java.time.Instant;
import java.util.Optional;

import ch.cern.spark.metrics.defined.equation.ValueComputable;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;

public class BooleanValue extends Value implements ValueComputable{

	private static final long serialVersionUID = 6026199196915653369L;

	private boolean bool;
	
	public BooleanValue(boolean value){
		this.bool = value;
	}

	@Override
	public Optional<Boolean> getAsBoolean() {
		return Optional.of(bool);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (bool ? 1231 : 1237);
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
		BooleanValue other = (BooleanValue) obj;
		if (bool != other.bool)
			return false;
		return true;
	}

	public static BooleanValue from(String value_string) {
		return new BooleanValue(Boolean.parseBoolean(value_string));
	}

	@Override
	public BooleanValue compute(VariableStatuses store, Instant time) {
		return new BooleanValue(bool);
	}

	@Override
	public Class<BooleanValue> returnType() {
		return BooleanValue.class;
	}

	@Override
	public String toString() {
		return Boolean.toString(bool);
	}
	
	@Override
	public String getSource() {
		if(source == null)
			return toString();
		else
			return super.toString();
	}
	
}
