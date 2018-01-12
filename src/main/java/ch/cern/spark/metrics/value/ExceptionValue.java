package ch.cern.spark.metrics.value;

import java.time.Instant;
import java.util.Optional;

import ch.cern.spark.metrics.defined.equation.ValueComputable;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(callSuper=false)
public class ExceptionValue extends Value implements ValueComputable{

	private static final long serialVersionUID = 8938782791564766439L;

	private String exception_message;

	public ExceptionValue(String message) {
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
