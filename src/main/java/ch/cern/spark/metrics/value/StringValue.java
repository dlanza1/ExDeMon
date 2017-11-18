package ch.cern.spark.metrics.value;

import java.time.Instant;
import java.util.Optional;

import ch.cern.spark.metrics.defined.DefinedMetricStore;
import ch.cern.spark.metrics.defined.equation.ValueComputable;

public class StringValue extends Value implements ValueComputable{

	private static final long serialVersionUID = 6026199196915653369L;

	private String stringValue;
	
	public StringValue(String value){
		this.stringValue = value;
	}

	@Override
	public Optional<String> getAsString() {
		return Optional.of(stringValue);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((stringValue == null) ? 0 : stringValue.hashCode());
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
		if (stringValue == null) {
			if (other.stringValue != null)
				return false;
		} else if (!stringValue.equals(other.stringValue))
			return false;
		return true;
	}

	@Override
	public StringValue compute(DefinedMetricStore store, Instant time) {
		return new StringValue(stringValue);
	}
	
	@Override
	public Class<StringValue> returnType() {
		return StringValue.class;
	}
	
	@Override
	public String toString() {
		return "\"" + stringValue + "\"";
	}
	
	@Override
	public String getSource() {
		if(source == null)
			return toString();
		else
			return super.toString();
	}

}
