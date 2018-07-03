package ch.cern.exdemon.metrics.defined.equation.functions.string;

import java.text.ParseException;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.functions.Function;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.metrics.value.Value;

public abstract class StringFunction extends Function{

	public static Class<? extends Value>[] argumentTypes = types(StringValue.class);

	public StringFunction(String representation, ValueComputable... arguments)
			throws ParseException {
		super(representation, argumentTypes, arguments);
	}

	@Override
	protected Value compute(Value... values) {
		return new StringValue(compute(values[0].getAsString().get()));
	}
	
	@Override
	public Class<StringValue> returnType() {
		return StringValue.class;
	}
	
	public abstract String compute(String value);

}
