package ch.cern.spark.metrics.defined.equation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.time.Instant;

import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.defined.equation.var.MetricVariableStatus;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.StringValue;
import ch.cern.spark.metrics.value.Value;

public class EquationTest {

	@Test
	public void evalWithLiterals() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		
		assertEquals(30f, new Equation("(5+10)*2", props).compute(null, null).getAsFloat().get(), 0.000f);
		assertEquals(45f, new Equation("(5+10) * (3)", props).compute(null, null).getAsFloat().get(), 0.000f);
		assertEquals(35f, new Equation("5+10 * (3)", props).compute(null, null).getAsFloat().get(), 0.000f);
		assertEquals(-2.5f, new Equation("(5-10)/2", props).compute(null, null).getAsFloat().get(), 0.000f);
		assertEquals(-2.5f, new Equation("(5-+10)/2", props).compute(null, null).getAsFloat().get(), 0.000f);
		assertEquals(7.5f, new Equation("(5--10)/2", props).compute(null, null).getAsFloat().get(), 0.000f);
		
		assertTrue(new Equation("true && true", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("true || false", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("false || true", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("false || false", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("!false || !false", props).compute(null, null).getAsBoolean().get());
		assertEquals(1f, new Equation("if_float(true, 1, 2)", props).compute(null, null).getAsFloat().get(), 0f);
		assertEquals(2f, new Equation("if_float(false, 1, 2)", props).compute(null, null).getAsFloat().get(), 0f);
		assertTrue(new Equation("if_bool(true, true, false)", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("if_bool(false, true, false)", props).compute(null, null).getAsBoolean().get());
		assertEquals("true", new Equation("if_string(true, \"true\", \"false\")", props).compute(null, null).getAsString().get());
		assertEquals("false", new Equation("if_string(false, \"true\", \"false\")", props).compute(null, null).getAsString().get());
		
		assertTrue(new Equation("true == true", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("1.23 == 1.23", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\"aa\" == \"aa\"", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\"a\\\"a\" == \"a\\\"a\"", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\'a\\\'a\' == \'a\\\'a\'", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\"a\\a\" == \"a\\a\"", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("true == 34", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("true == false", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("1.23 == 1.2343", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("1.23 == \"aa\"", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("\"aa\" == \"bb\"", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("\"a\\\"a\" == \"b\\\"b\"", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("\'a\\\'a\' == \'b\\\'b\'", props).compute(null, null).getAsBoolean().get());
		assertFalse(new Equation("\"a\\a\" == \"b\\b\"", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("true != 34", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("true != false", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("1.23 != 1.2343", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("1.23 != \"aa\"", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\"aa\" != \"bb\"", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\"a\\\"a\" != \"b\\\"b\"", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\'a\\\'a\' != \'b\\\'b\'", props).compute(null, null).getAsBoolean().get());
		assertTrue(new Equation("\"a\\a\" != \"b\\b\"", props).compute(null, null).getAsBoolean().get());
		
		String inputToTrim = "  a abc cde zz ";
		assertEquals(inputToTrim.trim(), new Equation("trim(\"" + inputToTrim + "\")", props).compute(null, null).getAsString().get());
		
		String inputToConcat1 = "aa";
		String inputToConcat2 = "bb";
		assertEquals(inputToConcat1 + inputToConcat2, new Equation("concat(\"" + inputToConcat1 + "\", \"" + inputToConcat2 + "\")", props).compute(null, null).getAsString().get());
	}
	
	@Test
	public void evalWithVariables() throws ParseException, ConfigurationException {
		Instant time = Instant.now();
		Properties props = new Properties();
		
		VariableStatuses stores = new VariableStatuses();
		MetricVariableStatus var1store = new MetricVariableStatus();
		stores.put("var1", var1store);
		MetricVariableStatus var2store = new MetricVariableStatus();
		stores.put("var2", var2store);
		MetricVariableStatus xstore = new MetricVariableStatus();
		stores.put("x", xstore);
		MetricVariableStatus ystore = new MetricVariableStatus();
		stores.put("y", ystore);
		
		props = new Properties();
		props.setProperty("var1.filter.attribute.A", "A");
		var1store.updateValue(new FloatValue(3), time);	
		assertEquals(39f, new Equation("(var1+10) * (var1)", props).compute(stores, time).getAsFloat().get(), 0.000f);
		
		var1store.updateValue(new FloatValue(3), time);	
		assertEquals(27f, new Equation("var1 ^ var1", props).compute(stores, time).getAsFloat().get(), 0.000f);
		assertEquals(27f, new Equation("3 ^ var1", props).compute(stores, time).getAsFloat().get(), 0.000f);
		assertEquals(27f, new Equation("var1 ^ 3", props).compute(stores, time).getAsFloat().get(), 0.000f);
	
		var1store.updateValue(new FloatValue(3), time);	
		assertEquals(3f, new Equation("var1", props).compute(stores, time).getAsFloat().get(), 0.000f);
		
		props.setProperty("var2.filter.attribute.B", "B");
		var1store.updateValue(new FloatValue(5), time);
		var2store.updateValue(new FloatValue(10), time);
		assertEquals(35f, new Equation("var1 + var2 * (3)", props).compute(stores, time).getAsFloat().get(), 0.000f);
		
		var1store.updateValue(new FloatValue(5), time);
		var2store.updateValue(new FloatValue(10), time);
		assertTrue(new Equation("(var1 == 5) && (var2 == 10) ", props).compute(stores, time).getAsBoolean().get());
		assertTrue(new Equation("(var1 == 5) && ((var2 * 2) == 20)", props).compute(stores, time).getAsBoolean().get());
		assertFalse(new Equation("(var1 == 5) && ((var2 * 2 + 1) == 20)", props).compute(stores, time).getAsBoolean().get());
		assertTrue(new Equation("(var1 == 5) && !((var2 * 2 + 1) == 20)", props).compute(stores, time).getAsBoolean().get());
		assertTrue(new Equation("(var2 == 10) && ((var2 * 2) == 20)", props).compute(stores, time).getAsBoolean().get());
		
		var1store.updateValue(new StringValue("/tmp/"), time);
		var2store.updateValue(new FloatValue(10), time);
		assertTrue(new Equation("(var1 == '/tmp/') && (var2 == 10)", props).compute(stores, time).getAsBoolean().get());
		assertTrue(new Equation("(var1 == \"/tmp/\") && (var2 == 10)", props).compute(stores, time).getAsBoolean().get());
		assertFalse(new Equation("(var1 != \"/tmp/\") && (var2 == 10)", props).compute(stores, time).getAsBoolean().get());
		assertFalse(new Equation("(var1 == \"/tmp/\") && (var2 == 11)", props).compute(stores, time).getAsBoolean().get());
		
		props.setProperty("x.filter.attribute.B", "B");
		props.setProperty("y.filter.attribute.B", "B");
		xstore.updateValue(new FloatValue(5), time);
		ystore.updateValue(new StringValue("10"), time);
		Value result = new Equation("x + y", props).compute(stores, time);
		assertTrue(result.getAsException().isPresent());
		assertEquals("Function \"+\": argument 2: requires float value", result.getAsException().get());
		assertEquals("(var(x)=5.0 + var(y)=\"10\")={Error: argument 2: requires float value}", result.getSource());
		
		xstore.updateValue(new FloatValue(5), time);
		ystore.updateValue(new FloatValue(10), time);
		var1store.updateValue(new FloatValue(10), time);
		assertEquals(105f, new Equation("x+y * (var1)", props).compute(stores, time).getAsFloat().get(), 0.000f);
		
		xstore.updateValue(new FloatValue(5), time);
		ystore.updateValue(new FloatValue(10), time);
		var1store.updateValue(new BooleanValue(true), time);
		var2store.updateValue(new BooleanValue(false), time);
		assertTrue(new Equation("(x + y) == 15 && (var1 == true) && (var2 != true)", props).compute(stores, time).getAsBoolean().get());
		assertEquals(5, new Equation("if_float(var1, x, y)", props).compute(stores, time).getAsFloat().get(), 0f);
		assertEquals(10, new Equation("if_float(var2, x, y)", props).compute(stores, time).getAsFloat().get(), 0f);
		assertEquals(15, new Equation("if_float(var1, x, 0) + if_float(!var2, y, 0)", props).compute(stores, time).getAsFloat().get(), 0f);
		assertEquals(15, new Equation("if_float(var1, x, y) + if_float(!var1, x, y)", props).compute(stores, time).getAsFloat().get(), 0f);
		
		String inputToConcat1 = "aa";
		String inputToConcat2 = "bb";
		var1store.updateValue(new StringValue(inputToConcat1), time);
		var2store.updateValue(new StringValue(inputToConcat2), time);
		assertEquals(inputToConcat1 + inputToConcat2, new Equation("concat(var1, var2)", props).compute(stores, time).getAsString().get());
	}

	@Test
	public void evalWithVariablesAndFormulas() throws ParseException, ConfigurationException {
		Instant time = Instant.now();
		Properties props = new Properties();;
		props.setProperty("x.filter.attribute.A", "A");
		
		VariableStatuses stores = new VariableStatuses();
		MetricVariableStatus xstore = new MetricVariableStatus();
		stores.put("x", xstore);
		MetricVariableStatus ystore = new MetricVariableStatus();
		stores.put("y", ystore);
		
		xstore.updateValue(new FloatValue(9), time);
		assertEquals(9f, new Equation("abs(x)", props).compute(stores, time).getAsFloat().get(), 0.01f);
		xstore.updateValue(new FloatValue(-9), time);
		assertEquals(9f, new Equation("abs(x)", props).compute(stores, time).getAsFloat().get(), 0.01f);
		
		xstore.updateValue(new FloatValue(10), time);
		assertEquals(3.16f, new Equation("sqrt(x)", props).compute(stores, time).getAsFloat().get(), 0.01f);
		
		xstore.updateValue(new FloatValue(10), time);
		assertEquals(0.17f, new Equation("sin(x)", props).compute(stores, time).getAsFloat().get(), 0.01f);
		
		xstore.updateValue(new FloatValue(10), time);
		assertEquals(0.98f, new Equation("cos(x)", props).compute(stores, time).getAsFloat().get(), 0.01f);
		
		xstore.updateValue(new FloatValue(10), time);
		assertEquals(0.17f, new Equation("tan(x)", props).compute(stores, time).getAsFloat().get(), 0.01f);
		
		props.setProperty("y.filter.attribute.A", "A");
		xstore.updateValue(new FloatValue(10), time);
		ystore.updateValue(new FloatValue(2), time);
		assertEquals(2.57f, new Equation("sin(x) + cos(x) + sqrt(y)", props).compute(stores, time).getAsFloat().get(), 0.01f);
	}
	
}
