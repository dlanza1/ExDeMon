package ch.cern.spark.metrics.value;

import java.time.Instant;
import java.util.Optional;

import ch.cern.spark.metrics.defined.equation.ValueComputable;
import ch.cern.spark.metrics.defined.equation.var.VariableStores;

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
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((exception_message == null) ? 0 : exception_message.hashCode());
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
		ExceptionValue other = (ExceptionValue) obj;
		if (exception_message == null) {
			if (other.exception_message != null)
				return false;
		} else if (!exception_message.equals(other.exception_message))
			return false;
		return true;
	}

	@Override
	public ExceptionValue compute(VariableStores store, Instant time) {
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
