package ch.cern.spark.metrics.defined;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

public class EquationTest {

	@Test
	public void eval() {
		assertEquals(30f, new Equation("(5+10)*2").compute(null).get(), 0.000f);
		assertEquals(45f, new Equation("(5+10) * (3)").compute(null).get(), 0.000f);
		assertEquals(35f, new Equation("5+10 * (3)").compute(null).get(), 0.000f);
	}
	
	@Test
	public void evalWithVariables() {
		Map<String, Float> vars = new HashMap<>();
		
		vars.put("x", 10f);
		assertEquals(30f, new Equation("(5+x)*2").compute(vars).get(), 0.000f);
		
		vars.put("var1", 3f);
		assertEquals(39f, new Equation("(var1+10) * (var1)").compute(vars).get(), 0.000f);
		
		vars.put("var1", 5f);
		vars.put("var2", 10f);
		assertEquals(35f, new Equation("var1 + var2 * (3)").compute(vars).get(), 0.000f);
		
		vars.put("x", 5f);
		vars.put("y", 10f);
		assertFalse(new Equation("x+y * (z)").compute(vars).isPresent());
	}
	
	@Test
	public void evalWithVariablesAndFormulas() {
		Map<String, Float> vars = new HashMap<>();
		
		vars.put("x", 10f);
		assertEquals(3.16f, new Equation("sqrt(x)").compute(vars).get(), 0.01f);
		
		vars.put("x", 10f);
		assertEquals(0.17f, new Equation("sin(x)").compute(vars).get(), 0.01f);
		
		vars.put("x", 10f);
		assertEquals(0.98f, new Equation("cos(x)").compute(vars).get(), 0.01f);
		
		vars.put("x", 10f);
		assertEquals(0.17f, new Equation("tan(x)").compute(vars).get(), 0.01f);
		
		vars.put("x", 10f);
		vars.put("y", 2f);
		assertEquals(2.57f, new Equation("sin(x) + cos(x) + sqrt(y)").compute(vars).get(), 0.01f);
	}
	
}
