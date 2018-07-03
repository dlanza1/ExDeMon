package ch.cern.exdemon.metrics.defined.equation.functions.num;

import java.text.ParseException;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;

public class MinusFunc extends NumericFunction {
	
	public static String REPRESENTATION = "-";

	public MinusFunc(ValueComputable... v) throws ParseException {
		super(REPRESENTATION, v);
		
		operationInTheMiddleForToString();
	}

	@Override
	public float compute(float value) {
		return -value;
	}

}
