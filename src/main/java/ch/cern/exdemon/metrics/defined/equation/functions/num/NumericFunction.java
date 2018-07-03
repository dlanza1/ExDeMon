package ch.cern.exdemon.metrics.defined.equation.functions.num;

import java.text.ParseException;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.functions.Function;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.Value;

public abstract class NumericFunction extends Function {

	public static Class<? extends Value>[] argumentTypes = types(FloatValue.class);

	public NumericFunction(String representation, ValueComputable... argument)
			throws ParseException {
		super(representation, argumentTypes, argument);
	}

	@Override
	protected Value compute(Value... values) {
		return new FloatValue(compute(values[0].getAsFloat().get()));
	}
	
	@Override
	public Class<FloatValue> returnType() {
		return FloatValue.class;
	}
	
	public abstract float compute(float value);
	
}
