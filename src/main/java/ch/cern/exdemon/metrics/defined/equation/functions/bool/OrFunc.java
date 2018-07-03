package ch.cern.exdemon.metrics.defined.equation.functions.bool;

import java.text.ParseException;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.functions.Function;
import ch.cern.exdemon.metrics.value.BooleanValue;
import ch.cern.exdemon.metrics.value.Value;

public class OrFunc extends Function {
	
	public static String REPRESENTATION = "||";
	
	public static Class<? extends Value>[] argumentTypes = types(BooleanValue.class, BooleanValue.class);

	public OrFunc(ValueComputable... arguments) throws ParseException {
		super(REPRESENTATION, argumentTypes, arguments);

		operationInTheMiddleForToString();
	}
	
	@Override
	protected Value compute(Value... values) {
		return new BooleanValue(values[0].getAsBoolean().get() || values[1].getAsBoolean().get());
	}

	@Override
	public Class<? extends Value> returnType() {
		return BooleanValue.class;
	}

}
