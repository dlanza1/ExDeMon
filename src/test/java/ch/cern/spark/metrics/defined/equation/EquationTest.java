package ch.cern.spark.metrics.defined.equation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.time.Instant;

import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.defined.DefinedMetricStore;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.StringValue;
import ch.cern.spark.metrics.value.Value;

public class EquationTest {

	@Test
	public void evalWithLoterals() throws ParseException, ConfigurationException {
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
		props.setProperty("x", "");
		props.setProperty("y", "");
		props.setProperty("var1", "");
		props.setProperty("var2", "");
		DefinedMetricStore store = new DefinedMetricStore();;
		
		store.updateValue("var1", new FloatValue(3), time);	
		assertEquals(39f, new Equation("(var1+10) * (var1)", props).compute(store, time).getAsFloat().get(), 0.000f);
		
		store.updateValue("var1", new FloatValue(3), time);	
		assertEquals(27f, new Equation("var1 ^ var1", props).compute(store, time).getAsFloat().get(), 0.000f);
		assertEquals(27f, new Equation("3 ^ var1", props).compute(store, time).getAsFloat().get(), 0.000f);
		assertEquals(27f, new Equation("var1 ^ 3", props).compute(store, time).getAsFloat().get(), 0.000f);
	
		store.updateValue("var1", new FloatValue(3), time);	
		assertEquals(3f, new Equation("var1", props).compute(store, time).getAsFloat().get(), 0.000f);
		
		store.updateValue("var1", new FloatValue(5), time);
		store.updateValue("var2", new FloatValue(10), time);
		assertEquals(35f, new Equation("var1 + var2 * (3)", props).compute(store, time).getAsFloat().get(), 0.000f);
		
		store.updateValue("var1", new FloatValue(5), time);
		store.updateValue("var2", new FloatValue(10), time);
		assertTrue(new Equation("(var1 == 5) && (var2 == 10) ", props).compute(store, time).getAsBoolean().get());
		assertTrue(new Equation("(var1 == 5) && ((var2 * 2) == 20)", props).compute(store, time).getAsBoolean().get());
		assertFalse(new Equation("(var1 == 5) && ((var2 * 2 + 1) == 20)", props).compute(store, time).getAsBoolean().get());
		assertTrue(new Equation("(var1 == 5) && !((var2 * 2 + 1) == 20)", props).compute(store, time).getAsBoolean().get());
		assertTrue(new Equation("(var2 == 10) && ((var2 * 2) == 20)", props).compute(store, time).getAsBoolean().get());
		
		store.updateValue("var1", new StringValue("/tmp/"), time);
		store.updateValue("var2", new FloatValue(10), time);
		assertTrue(new Equation("(var1 == '/tmp/') && (var2 == 10)", props).compute(store, time).getAsBoolean().get());
		assertTrue(new Equation("(var1 == \"/tmp/\") && (var2 == 10)", props).compute(store, time).getAsBoolean().get());
		assertFalse(new Equation("(var1 != \"/tmp/\") && (var2 == 10)", props).compute(store, time).getAsBoolean().get());
		assertFalse(new Equation("(var1 == \"/tmp/\") && (var2 == 11)", props).compute(store, time).getAsBoolean().get());
		
		store.updateValue("x", new FloatValue(5), time);
		store.updateValue("y", new StringValue("10"), time);
		Value result = new Equation("x + y", props).compute(store, time);
		assertTrue(result.getAsException().isPresent());
		assertEquals("Function \"+\": argument 2: requires float value", result.getAsException().get());
		assertEquals("(var(x)=5.0 + var(y)=\"10\")={Error: argument 2: requires float value}", result.getSource());
		
		store.updateValue("x", new FloatValue(5), time);
		store.updateValue("y", new FloatValue(10), time);
		store.updateValue("var1", new FloatValue(10), time);
		assertEquals(105f, new Equation("x+y * (var1)", props).compute(store, time).getAsFloat().get(), 0.000f);
		
		store.updateValue("x", new FloatValue(5), time);
		store.updateValue("y", new FloatValue(10), time);
		store.updateValue("var1", new BooleanValue(true), time);
		store.updateValue("var2", new BooleanValue(false), time);
		assertTrue(new Equation("(x + y) == 15 && (var1 == true) && (var2 != true)", props).compute(store, time).getAsBoolean().get());
		assertEquals(5, new Equation("if_float(var1, x, y)", props).compute(store, time).getAsFloat().get(), 0f);
		assertEquals(10, new Equation("if_float(var2, x, y)", props).compute(store, time).getAsFloat().get(), 0f);
		assertEquals(15, new Equation("if_float(var1, x, 0) + if_float(!var2, y, 0)", props).compute(store, time).getAsFloat().get(), 0f);
		assertEquals(15, new Equation("if_float(var1, x, y) + if_float(!var1, x, y)", props).compute(store, time).getAsFloat().get(), 0f);
		
		String inputToConcat1 = "aa";
		String inputToConcat2 = "bb";
		store.updateValue("var1", new StringValue(inputToConcat1), time);
		store.updateValue("var2", new StringValue(inputToConcat2), time);
		assertEquals(inputToConcat1 + inputToConcat2, new Equation("concat(var1, var2)", props).compute(store, time).getAsString().get());
	}

	@Test
	public void evalWithVariablesAndFormulas() throws ParseException, ConfigurationException {
		Instant time = Instant.now();
		Properties props = new Properties();;
		props.setProperty("x", "");
		props.setProperty("y", "");
		DefinedMetricStore store = new DefinedMetricStore();
		
		store.updateValue("x", new FloatValue(9), time);
		assertEquals(9f, new Equation("abs(x)", props).compute(store, time).getAsFloat().get(), 0.01f);
		store.updateValue("x", new FloatValue(-9), time);
		assertEquals(9f, new Equation("abs(x)", props).compute(store, time).getAsFloat().get(), 0.01f);
		
		store.updateValue("x", new FloatValue(10), time);
		assertEquals(3.16f, new Equation("sqrt(x)", props).compute(store, time).getAsFloat().get(), 0.01f);
		
		store.updateValue("x", new FloatValue(10), time);
		assertEquals(0.17f, new Equation("sin(x)", props).compute(store, time).getAsFloat().get(), 0.01f);
		
		store.updateValue("x", new FloatValue(10), time);
		assertEquals(0.98f, new Equation("cos(x)", props).compute(store, time).getAsFloat().get(), 0.01f);
		
		store.updateValue("x", new FloatValue(10), time);
		assertEquals(0.17f, new Equation("tan(x)", props).compute(store, time).getAsFloat().get(), 0.01f);
		
		store.updateValue("x", new FloatValue(10), time);
		store.updateValue("y", new FloatValue(2), time);
		assertEquals(2.57f, new Equation("sin(x) + cos(x) + sqrt(y)", props).compute(store, time).getAsFloat().get(), 0.01f);
	}
	
}
