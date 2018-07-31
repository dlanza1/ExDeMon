package ch.cern.exdemon.metrics.defined.equation.functions.num;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.time.Instant;

import org.junit.Test;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.exdemon.metrics.value.BooleanValue;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.metrics.value.Value;

public class BiNumericFunctionTest {
	
	@Test
	public void shouldCompute() throws ParseException {
		ValueComputable value = new ValueComputable() {
			
			@Override
			public Class<? extends Value> returnType() {
				return FloatValue.class;
			}
			
			@Override
			public Value compute(VariableStatuses store, Instant time) {
				return new FloatValue(10);
			}
		};
		
		BiNumericFunctionImp func = new BiNumericFunctionImp(value, value);
		
		Value result = func.compute(null, null);
		
		assertTrue(result.getAsFloat().isPresent());
		assertEquals(20, result.getAsFloat().get(), 0.0f);
	}
	
	@Test
	public void shouldReturnExceptionValueIfExceptionValuesAreReceived() throws ParseException {
		ValueComputable exceptionValue = new ValueComputable() {
			
			@Override
			public Class<? extends Value> returnType() {
				return FloatValue.class;
			}
			
			@Override
			public Value compute(VariableStatuses store, Instant time) {
				return new ExceptionValue("exception message");
			}
		};
		
		ValueComputable floatValue = new ValueComputable() {
			
			@Override
			public Class<? extends Value> returnType() {
				return FloatValue.class;
			}
			
			@Override
			public Value compute(VariableStatuses store, Instant time) {
				return new FloatValue(10);
			}
		};
		
		BiNumericFunctionImp func = new BiNumericFunctionImp(exceptionValue, floatValue);
		Value result = func.compute(null, null);
		assertTrue(result.getAsException().isPresent());
		assertEquals("exception message", result.getAsException().get());
		assertEquals("testFunc({Error: exception message}, 10.0)={Error: in arguments}", result.getSource());
		
		func = new BiNumericFunctionImp(floatValue, exceptionValue);
		result = func.compute(null, null);
		assertTrue(result.getAsException().isPresent());
		assertEquals("exception message", result.getAsException().get());
		assertEquals("testFunc(10.0, {Error: exception message})={Error: in arguments}", result.getSource());
		
		func = new BiNumericFunctionImp(exceptionValue, exceptionValue);
		result = func.compute(null, null);
		assertTrue(result.getAsException().isPresent());
		assertEquals("exception message", result.getAsException().get());
		assertEquals("testFunc({Error: exception message}, {Error: exception message})={Error: in arguments}", result.getSource());
	}
	
	@Test
	public void shouldShowSeveralExceptionsWithTypes() throws ParseException {
		ValueComputable exceptionValue = new ValueComputable() {
			
			@Override
			public Class<? extends Value> returnType() {
				return FloatValue.class;
			}
			
			@Override
			public Value compute(VariableStatuses store, Instant time) {
				return new BooleanValue(true);
			}
		};
		
		ValueComputable stringValue = new ValueComputable() {
			
			@Override
			public Class<? extends Value> returnType() {
				return FloatValue.class;
			}
			
			@Override
			public Value compute(VariableStatuses store, Instant time) {
				return new StringValue("");
			}
		};
		
		BiNumericFunctionImp func = new BiNumericFunctionImp(exceptionValue, stringValue);
		Value result = func.compute(null, null);
		assertTrue(result.getAsException().isPresent());
		assertEquals("Function \"testFunc\": argument 1: requires float value, argument 2: requires float value", result.getAsException().get());
		assertEquals("testFunc(true, \"\")={Error: argument 1: requires float value, argument 2: requires float value}", result.getSource());
	}
	
	private static class BiNumericFunctionImp extends BiNumericFunction{

		public BiNumericFunctionImp(ValueComputable v1, ValueComputable v2) throws ParseException {
			super("testFunc", v1, v2);
		}

		@Override
		public float compute(float value1, float value2) {
			return value1 + value2;
		}
		
	}
	
}
