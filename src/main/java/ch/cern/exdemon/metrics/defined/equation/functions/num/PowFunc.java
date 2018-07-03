package ch.cern.exdemon.metrics.defined.equation.functions.num;

import java.text.ParseException;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;

public class PowFunc extends BiNumericFunction{
	
	public static String REPRESENTATION = "^";

	public PowFunc(ValueComputable... arguments) throws ParseException {
		super(REPRESENTATION, arguments);
		
		operationInTheMiddleForToString();
	}

	@Override
	public float compute(float value1, float value2) {
		return (float) Math.pow(value1, value2);
	}

}
