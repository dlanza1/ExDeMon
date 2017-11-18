package ch.cern.spark.metrics.defined.equation.functions.bool;

import java.text.ParseException;

import ch.cern.spark.metrics.defined.equation.ValueComputable;
import ch.cern.spark.metrics.defined.equation.functions.Function;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.Value;

public class AndFunc extends Function {
	
	public static String REPRESENTATION = "&&";
	
	public static Class<? extends Value>[] argumentTypes = types(BooleanValue.class, BooleanValue.class);

	public AndFunc(ValueComputable... arguments) throws ParseException {
		super(REPRESENTATION, argumentTypes, arguments);

		operationInTheMiddleForToString();
	}

	@Override
	public Class<? extends Value> returnType() {
		return BooleanValue.class;
	}

	@Override
	protected Value compute(Value... values) {
		return new BooleanValue(values[0].getAsBoolean().get() && values[1].getAsBoolean().get());
	}

}
