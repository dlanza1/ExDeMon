package ch.cern.exdemon.metrics.defined.equation.functions.num;

import java.text.ParseException;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.functions.FunctionCaller;
import ch.cern.exdemon.metrics.value.Value;

public class SqrtFunc extends NumericFunction {
	
	public static String REPRESENTATION = "sqrt";

	public SqrtFunc(ValueComputable... arguments) throws ParseException {
		super(REPRESENTATION, arguments);
	}

	@Override
	public float compute(float value) {
		return (float) Math.sqrt(value);
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
			return new SqrtFunc(arguments);
		}
		
	}
	
}
