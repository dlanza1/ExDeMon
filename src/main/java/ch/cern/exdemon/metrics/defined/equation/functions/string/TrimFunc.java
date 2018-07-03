package ch.cern.exdemon.metrics.defined.equation.functions.string;

import java.text.ParseException;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.functions.FunctionCaller;
import ch.cern.exdemon.metrics.value.Value;

public class TrimFunc extends StringFunction {
	
	public static String REPRESENTATION = "trim";

	public TrimFunc(ValueComputable... v) throws ParseException {
		super(REPRESENTATION, v);
	}
	
	@Override
	public String compute(String value) {
		return value.trim();
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
			return new TrimFunc(arguments);
		}
		
	}
	
}
