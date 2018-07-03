package ch.cern.exdemon.metrics.defined.equation.functions.num;

import java.text.ParseException;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.functions.Function;
import ch.cern.exdemon.metrics.defined.equation.functions.FunctionCaller;
import ch.cern.exdemon.metrics.value.BooleanValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.Value;

public class IfFloatFunc extends Function {

	public static String REPRESENTATION = "if_float";

	public static Class<? extends Value>[] argumentTypes = types(BooleanValue.class, FloatValue.class, FloatValue.class);

	public IfFloatFunc(ValueComputable... arguments)
			throws ParseException {
		super(REPRESENTATION, argumentTypes, arguments);
	}

	@Override
	protected Value compute(Value... values) {
		return values[0].getAsBoolean().get() ? values[1] : values[2];
	}

	@Override
	public Class<? extends Value> returnType() {
		return FloatValue.class;
	}
	
	public static class Caller implements FunctionCaller{
		
		@Override
		public String getFunctionRepresentation() {
			return REPRESENTATION;
		}

		@Override
		public Class<? extends Value>[] getArgumentTypes() {
			return argumentTypes;
		}

		@Override
		public ValueComputable call(ValueComputable... arguments) throws ParseException {
			return new IfFloatFunc(arguments);
		}
		
	}
	
}
