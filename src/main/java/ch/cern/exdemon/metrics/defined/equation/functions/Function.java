package ch.cern.exdemon.metrics.defined.equation.functions;

import java.text.ParseException;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.exdemon.metrics.value.BooleanValue;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.metrics.value.Value;

public abstract class Function implements ValueComputable{
	
	private String representation;
	
	private Class<? extends Value>[] types;
	
	protected ValueComputable[] arguments;

	private boolean operationInTheMiddle = false;	

	public Function(String representation, Class<? extends Value>[] types, ValueComputable... arguments) throws ParseException {
		this.representation = representation;
		
		if(types.length != arguments.length)
			throw new ParseException(getExceptionPrefix() + "expects " + types.length + " arguments", 0);
		
		this.types = types;
		this.arguments = arguments;
		
		for (int i = 0; i < arguments.length; i++) {
			Class<? extends Value> argumentsReturnType = arguments[i].returnType();
			
			if(!types[i].equals(Value.class) && !argumentsReturnType.equals(types[i]))
				throw new ParseException(getExceptionPrefix() + "expects type " + types[i].getSimpleName() + " for argument " + (i+1), 0);
		}
	}

	@Override
	public Value compute(VariableStatuses stores, Instant time) {
		Value[] argumentValues = new Value[arguments.length];
		
		Set<String> exceptions = new HashSet<>();
		String typeExceptions = "";
		for (int i = 0; i < arguments.length; i++) {
			ValueComputable argument = arguments[i];
			
			argumentValues[i] = argument.compute(stores, time);		
			if(argumentValues[i].getAsException().isPresent())
				exceptions.add(argumentValues[i].getAsException().get());
			else if(types[i].equals(FloatValue.class) && !argumentValues[i].getAsFloat().isPresent())
				typeExceptions += "argument " + (i+1) + ": requires float value, ";
			else if(types[i].equals(StringValue.class) && !argumentValues[i].getAsString().isPresent())
				typeExceptions += "argument " + (i+1) + ": requires string value, ";
			else if(types[i].equals(BooleanValue.class) && !argumentValues[i].getAsBoolean().isPresent())
				typeExceptions += "argument " + (i+1) + ": requires boolean value, ";
		}
		
		Value result = null;
		if(!exceptions.isEmpty()) {
			String exceptionsString = toString(exceptions);
			result = new ExceptionValue(exceptionsString);
			
			setSourceFromArgumentmValues(result, new ExceptionValue("in arguments").toString(), argumentValues);
		}else if(typeExceptions.length() > 0) {
			typeExceptions = typeExceptions.substring(0, typeExceptions.length() - 2);
			result = new ExceptionValue(getExceptionPrefix() + typeExceptions);
			
			setSourceFromArgumentmValues(result, new ExceptionValue(typeExceptions).toString(), argumentValues);
		}else{
			result = compute(argumentValues);
			setSourceFromArgumentmValues(result, result.toString(), argumentValues);
		}
		
		return result;
	}

	private String toString(Set<String> exceptions) {
		String exceptionsString = "";
		
		for (String exceptio : exceptions)
			exceptionsString += exceptio + ", ";
		
		return exceptionsString.substring(0, exceptionsString.length() - 2);
	}

	protected void setSourceFromArgumentmValues(Value result, String resultString, Value... argumentValues) {
		if(operationInTheMiddle) {
			result.setSource("(" + argumentValues[0].getSource() + " " + representation + " " + argumentValues[1].getSource() + ")=" + resultString);
		}else {
			String output = representation + "(";
			
			for (Value argumentValue : argumentValues)
				output += argumentValue.getSource() + ", ";
			
			result.setSource(output.substring(0, output.length() - 2) + ")=" + resultString);
		}
		
		List<Metric> lastSourceMetrics = Arrays.stream(argumentValues).map(value -> value.getLastSourceMetrics())
		                                                              .filter(metrics -> metrics != null)
		                                                              .flatMap(List::stream)
		                                                              .distinct()
		                                                              .collect(Collectors.toList());
		if(lastSourceMetrics.size() > 0)
            result.setLastSourceMetrics(lastSourceMetrics);
	}

	protected abstract Value compute(Value... values);

	protected String getExceptionPrefix() {
		return "Function \"" + representation + "\": ";
	}
	
	public String getRepresentation() {
		return representation;
	}
	
	@SafeVarargs
	protected static Class<? extends Value>[] types(Class<? extends Value>... types) {
		return types;
	}
	
	protected void operationInTheMiddleForToString() {
		if(arguments.length == 2)
			this.operationInTheMiddle  = true;
	}
	
	@Override
	public String toString() {
		if(operationInTheMiddle)
			return "(" + arguments[0] + " " + representation + " " + arguments[1] + ")";
		
		String output = representation + "(";
		
		for (ValueComputable argument : arguments)
			output += argument + ", ";
		
		return output.substring(0, output.length() - 2) + ")";
	}
	
}
