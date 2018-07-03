package ch.cern.exdemon.metrics.value;

import java.time.Instant;
import java.util.Optional;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper=false)
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
