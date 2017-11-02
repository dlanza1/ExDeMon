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
		Map<String, Double> vars = new HashMap<>();
		
		vars.put("x", 10d);
		assertEquals(30f, new Equation("(5+x)*2").compute(vars).get(), 0.000f);
		
		vars.put("var1", 3d);
		assertEquals(39f, new Equation("(var1+10) * (var1)").compute(vars).get(), 0.000f);
		
		vars.put("var1", 5d);
		vars.put("var2", 10d);
		assertEquals(35f, new Equation("var1 + var2 * (3)").compute(vars).get(), 0.000f);
		
		vars.put("x", 5d);
		vars.put("y", 10d);
		assertFalse(new Equation("x+y * (z)").compute(vars).isPresent());
	}
	
	@Test
	public void evalWithVariablesAndFormulas() {
		Map<String, Double> vars = new HashMap<>();
		
		vars.put("x", 9d);
		assertEquals(9f, new Equation("abs(x)").compute(vars).get(), 0.01f);
		vars.put("x", -9d);
		assertEquals(9f, new Equation("abs(x)").compute(vars).get(), 0.01f);
		
		vars.put("x", 10d);
		assertEquals(3.16f, new Equation("sqrt(x)").compute(vars).get(), 0.01f);
		
		vars.put("x", 10d);
		assertEquals(0.17f, new Equation("sin(x)").compute(vars).get(), 0.01f);
		
		vars.put("x", 10d);
		assertEquals(0.98f, new Equation("cos(x)").compute(vars).get(), 0.01f);
		
		vars.put("x", 10d);
		assertEquals(0.17f, new Equation("tan(x)").compute(vars).get(), 0.01f);
		
		vars.put("x", 10d);
		vars.put("y", 2d);
		assertEquals(2.57f, new Equation("sin(x) + cos(x) + sqrt(y)").compute(vars).get(), 0.01f);
	}
	
}
