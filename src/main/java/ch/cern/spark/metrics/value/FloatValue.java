package ch.cern.spark.metrics.value;

import java.time.Instant;
import java.util.Optional;

import ch.cern.spark.metrics.defined.equation.ValueComputable;
import ch.cern.spark.metrics.defined.equation.var.VariableStores;

public class FloatValue extends Value implements ValueComputable{

	private static final long serialVersionUID = 6026199196915653369L;

	private float num;
	
	public FloatValue(double value){
		this.num = (float) value;
	}
	
	@Override
	public FloatValue compute(VariableStores store, Instant time) {
		return new FloatValue(num);
	}

	@Override
	public Optional<Float> getAsFloat() {
		return Optional.of(this.num);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Float.floatToIntBits(num);
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
		FloatValue other = (FloatValue) obj;
		if (Float.floatToIntBits(num) != Float.floatToIntBits(other.num))
			return false;
		return true;
	}

	public static FloatValue from(String value_string) {
		return new FloatValue(Float.parseFloat(value_string));
	}

	@Override
	public Class<FloatValue> returnType() {
		return FloatValue.class;
	}
	
	@Override
	public String toString() {
		return Float.toString(num);
	}
	
	@Override
	public String getSource() {
		if(source == null)
			return toString();
		else
			return super.toString();
	}
	
}
