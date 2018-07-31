package ch.cern.exdemon.metrics.defined.equation.functions.num;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.time.Instant;

import org.junit.Test;

import ch.cern.exdemon.metrics.defined.equation.ValueComputable;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.metrics.value.Value;

public class NumericFunctionTest {
	
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
		
		NumericFunction func = new NumericFunctionImp(value);
		
		Value result = func.compute(null, null);
		
		assertTrue(result.getAsFloat().isPresent());
		assertEquals(10, result.getAsFloat().get(), 0.0f);
	}
	
	@Test
	public void shouldReturnExceptionValueIFDifferentType() throws ParseException {
		ValueComputable value = new ValueComputable() {
			
			@Override
			public Class<? extends Value> returnType() {
				return FloatValue.class;
			}
			
			@Override
			public Value compute(VariableStatuses store, Instant time) {
				return new StringValue("");
			}
		};
		
		NumericFunction func = new NumericFunctionImp(value);
		
		Value result = func.compute(null, null);
		
		assertTrue(result.getAsException().isPresent());
		assertEquals("Function \"\": argument 1: requires float value", result.getAsException().get());
		assertEquals("(\"\")={Error: argument 1: requires float value}", result.getSource());
	}
	
	@Test
	public void shouldReturnSameExceptionValueIfExceptionValueIsReceived() throws ParseException {
		ValueComputable value = new ValueComputable() {
			
			@Override
			public Class<? extends Value> returnType() {
				return FloatValue.class;
			}
			
			@Override
			public Value compute(VariableStatuses store, Instant time) {
				return new ExceptionValue("exception message");
			}
		};
		
		NumericFunction func = new NumericFunctionImp(value);
		
		Value result = func.compute(null, null);
		
		assertTrue(result.getAsException().isPresent());
		assertEquals("exception message", result.getAsException().get());
		assertEquals("({Error: exception message})={Error: in arguments}", result.getSource());
	}
	
	private static class NumericFunctionImp extends NumericFunction{

		public NumericFunctionImp(ValueComputable v) throws ParseException {
			super("", v);
		}

		@Override
		public float compute(float value) {
			return value;
		}
		
	}
	
}
