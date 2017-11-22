package ch.cern.spark.metrics.defined.equation.functions.analysis;

import java.text.ParseException;
import java.time.Instant;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.analysis.Analysis;
import ch.cern.spark.metrics.analysis.BooleanAnalysis;
import ch.cern.spark.metrics.analysis.NumericAnalysis;
import ch.cern.spark.metrics.analysis.StringAnalysis;
import ch.cern.spark.metrics.defined.equation.ValueComputable;
import ch.cern.spark.metrics.defined.equation.functions.Function;
import ch.cern.spark.metrics.defined.equation.functions.FunctionCaller;
import ch.cern.spark.metrics.defined.equation.var.VariableStores;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.PropertiesValue;
import ch.cern.spark.metrics.value.StringValue;
import ch.cern.spark.metrics.value.Value;

public class AnalysisFunc extends Function {
	
	public static String REPRESENTATION = "analysis";
	
	public static Class<? extends Value>[] argumentTypes = types(Value.class, PropertiesValue.class);
	
	private Analysis analysis;

	private PropertiesValue propsVal;

	public AnalysisFunc(ValueComputable... arguments) throws ParseException {
		super(REPRESENTATION, argumentTypes, arguments);
		
		propsVal = (PropertiesValue) arguments[1].compute(null, null);
		Properties props = propsVal.getAsProperties().get();
		
		try{
			analysis = ComponentManager.build(Type.ANAYLSIS, props);
			analysis.config(props);
		}catch(ConfigurationException e) {
			throw new ParseException(e.getClass().getSimpleName() + e.getMessage(), 0);
		}
	}

	@Override
	public Class<? extends Value> returnType() {
		return StringValue.class;
	}
	
	@Override
	public Value compute(VariableStores stores, Instant time) {
		Value result = null;
		
		Value value = arguments[0].compute(stores, time);
		
		if(value.getAsException().isPresent()) {
			result = new ExceptionValue(value.getAsException().get());
			
			setSourceFromArgumentmValues(result, new ExceptionValue("in arguments").toString(), value, propsVal);
			
			return result;
		}else if(analysis instanceof NumericAnalysis) {
			result = computeNumericAnalysis((NumericAnalysis) analysis, propsVal, value, time);
		}else if(analysis instanceof StringAnalysis) {
			result = computeStringAnalysis((StringAnalysis) analysis, propsVal, value, time);
		}else if(analysis instanceof BooleanAnalysis) {
			result = computeBooleanAnalysis((BooleanAnalysis) analysis, propsVal, value, time);
		}
		
		if(analysis instanceof HasStore)
			stores.put(propsVal.getName(), ((HasStore) analysis).save());
		
		return result;
	}
	
	private Value computeBooleanAnalysis(BooleanAnalysis analysis, Value props, Value value, Instant time) {
		Value result;
		
		if(!value.getAsBoolean().isPresent()) {
			result = new ExceptionValue(getExceptionPrefix() + "argument 1: requires boolean value");
			
			setSourceFromArgumentmValues(result, new ExceptionValue("argument 1: requires boolean value").toString(), value, props);
		}else{
			value.setSource("filter_boolean(" + value.getSource() + ")");
			
			AnalysisResult analysisResult = analysis.process(time, value.getAsBoolean().get());
			
			result = new StringValue(analysisResult.getStatus().toString());
			setSourceFromArgumentmValues(result, result.toString(), value, props);
		}
		
		return result;
	}

	private Value computeStringAnalysis(StringAnalysis analysis, Value props, Value value, Instant time) {
		Value result;
		
		if(!value.getAsString().isPresent()) {
			result = new ExceptionValue(getExceptionPrefix() + "argument 1: requires string value");
			
			setSourceFromArgumentmValues(result, new ExceptionValue("argument 1: requires string value").toString(), value, props);
		}else{
			value.setSource("filter_string(" + value.getSource() + ")");
			
			AnalysisResult analysisResult = analysis.process(time, value.getAsString().get());
			
			result = new StringValue(analysisResult.getStatus().toString());
			setSourceFromArgumentmValues(result, result.toString(), value, props);
		}
		
		return result;
	}

	private Value computeNumericAnalysis(NumericAnalysis analysis, Value props, Value value, Instant time) {
		Value result;
		
		if(!value.getAsFloat().isPresent()) {
			result = new ExceptionValue(getExceptionPrefix() + "argument 1: requires float value");
			
			setSourceFromArgumentmValues(result, new ExceptionValue("argument 1: requires float value").toString(), value, props);
		}else{
			value.setSource("filter_float(" + value.getSource() + ")");
			
			AnalysisResult analysisResult = analysis.process(time, value.getAsFloat().get());
			
			result = new StringValue(analysisResult.getStatus().toString());
			setSourceFromArgumentmValues(result, result.toString(), value, props);
		}
		
		return result;
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
			return new AnalysisFunc(arguments);
		}
		
	}

	@Override
	protected Value compute(Value... values) {
		throw new RuntimeException("It cannot be called");
	}

}
