package ch.cern.spark.metrics.value;

import java.time.Instant;
import java.util.Optional;

import ch.cern.spark.metrics.defined.DefinedMetricStore;
import ch.cern.spark.metrics.defined.equation.ValueComputable;

public class ExceptionValue extends Value implements ValueComputable{

	private static final long serialVersionUID = 8938782791564766439L;

	private String message;

	public ExceptionValue(String message) {
		this.message = message;
	}

	@Override
	public Optional<String> getAsException() {
		return Optional.of(message);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((message == null) ? 0 : message.hashCode());
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
		if (message == null) {
			if (other.message != null)
				return false;
		} else if (!message.equals(other.message))
			return false;
		return true;
	}

	@Override
	public ExceptionValue compute(DefinedMetricStore store, Instant time) {
		return new ExceptionValue(message);
	}

	@Override
	public Class<ExceptionValue> returnType() {
		return ExceptionValue.class;
	}
	
	@Override
	public String toString() {
		return "{Error: " + message + "}";
	}
	
	@Override
	public String getSource() {
		if(source == null)
			return toString();
		else
			return super.toString();
	}

}
