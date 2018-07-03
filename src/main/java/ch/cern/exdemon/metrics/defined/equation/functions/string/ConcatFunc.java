package ch.cern.exdemon.metrics.defined.equation.functions.string;

import java.text.ParseException;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.functions.FunctionCaller;
import ch.cern.exdemon.metrics.value.Value;

public class ConcatFunc extends BiStringFunction {
	
	public static String REPRESENTATION = "concat";

	public ConcatFunc(ValueComputable... arguments) throws ParseException {
		super(REPRESENTATION, arguments);
	}
	
	@Override
	public String compute(String value1, String value2) {
		return value1.concat(value2);
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
			return new ConcatFunc(arguments);
		}
		
	}

}
