package ch.cern.exdemon.metrics.defined.equation.functions.analysis;

import java.text.ParseException;
import java.time.Instant;
import java.util.HashMap;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentBuildResult;
import ch.cern.exdemon.components.ComponentTypes;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.functions.Function;
import ch.cern.exdemon.metrics.defined.equation.functions.FunctionCaller;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatus;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.exdemon.metrics.value.PropertiesValue;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.exdemon.monitor.analysis.Analysis;
import ch.cern.exdemon.monitor.analysis.BooleanAnalysis;
import ch.cern.exdemon.monitor.analysis.NumericAnalysis;
import ch.cern.exdemon.monitor.analysis.StringAnalysis;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.types.NoneAnalysis;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.HasStatus;
import ch.cern.spark.status.StatusValue;

public class AnalysisFunc extends Function {
	
	public static String REPRESENTATION = "analysis";
	
	public static Class<? extends Value>[] argumentTypes = types(Value.class, PropertiesValue.class);
	
	private Analysis analysis;

	private PropertiesValue propsVal;

	public AnalysisFunc(ValueComputable... arguments) throws ParseException {
		super(REPRESENTATION, argumentTypes, arguments);
		
		propsVal = (PropertiesValue) arguments[1].compute(null, null);
		Properties props = propsVal.getAsProperties().get();
		
	    ComponentBuildResult<Analysis> analysisBuildResult = ComponentTypes.build(Type.ANAYLSIS, props);
	    try {
            analysisBuildResult.throwExceptionsIfPresent();
        } catch (ConfigurationException e) {
            throw new ParseException(e.getClass().getSimpleName() + ": " + e.getMessage(), 0);
        }
	    
		analysis = analysisBuildResult.getComponent().get();
	}

	@Override
	public Class<? extends Value> returnType() {
		return StringValue.class;
	}
	
	@Override
	public Value compute(VariableStatuses stores, Instant time) {
		Value result = null;
		
		Value value = arguments[0].compute(stores, time);
		
		if(analysis instanceof HasStatus) {
		    VariableStatus status = stores.get(propsVal.getName());
		    
		    if(status != null && status instanceof AnalysisStatus)		    
		        ((HasStatus) analysis).load(((AnalysisStatus) status).status);
        }
		
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
		}else if(analysis instanceof NoneAnalysis) {
			AnalysisResult analysisResult = analysis.apply(new Metric(time, value, new HashMap<>()));
			
			result = new StringValue(analysisResult.getStatus().toString());
			setSourceFromArgumentmValues(result, result.toString(), value, propsVal);
		}else {
			result = new ExceptionValue("Analysis not available for defined metrics..");
			
			setSourceFromArgumentmValues(result, new ExceptionValue("argument 1: requires boolean value").toString(), value, propsVal);
		}
		
		if(analysis instanceof HasStatus) {
		    AnalysisStatus status = new AnalysisStatus();
		    status.status = ((HasStatus) analysis).save();
		    
			stores.put(propsVal.getName(), status);
		}
		
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

	public static class AnalysisStatus extends VariableStatus {
        private static final long serialVersionUID = -2164984398629720406L;
        
        StatusValue status;
	}
	
}
