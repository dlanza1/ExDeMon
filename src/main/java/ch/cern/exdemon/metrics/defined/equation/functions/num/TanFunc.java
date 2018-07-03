package ch.cern.exdemon.metrics.defined.equation.functions.num;

import java.text.ParseException;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.functions.FunctionCaller;
import ch.cern.exdemon.metrics.value.Value;

public class TanFunc extends NumericFunction {
	
	public static String REPRESENTATION = "tan";

	public TanFunc(ValueComputable... v) throws ParseException {
		super(REPRESENTATION, v);
	}

	@Override
	public float compute(float value) {
		return (float) Math.tan(Math.toRadians(value));
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
			return new TanFunc(arguments);
		}
		
	}
	
}
