package ch.cern.exdemon.metrics.value;

import java.time.Instant;
import java.util.Optional;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import lombok.EqualsAndHashCode;
import lombok.NonNull;

@EqualsAndHashCode(callSuper=false)
public class ExceptionValue extends Value implements ValueComputable{

	private static final long serialVersionUID = 8938782791564766439L;

	private String exception_message;

	public ExceptionValue(@NonNull String message) {
		this.exception_message = message;
	}

	@Override
	public Optional<String> getAsException() {
		return Optional.of(exception_message);
	}

	@Override
	public ExceptionValue compute(VariableStatuses store, Instant time) {
		return new ExceptionValue(exception_message);
	}

	@Override
	public Class<ExceptionValue> returnType() {
		return ExceptionValue.class;
	}
	
	@Override
	public String toString() {
		return "{Error: " + exception_message + "}";
	}
	
	@Override
	public String getSource() {
		if(source == null)
			return toString();
		else
			return super.toString();
	}

}
