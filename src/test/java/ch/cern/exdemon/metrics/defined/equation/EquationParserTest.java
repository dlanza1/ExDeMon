package ch.cern.exdemon.metrics.defined.equation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.text.ParseException;

import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class EquationParserTest {

	@Test
	public void shouldNotParse() {
		Properties props = new Properties();
		props.setProperty("x", "");
		props.setProperty("y", "");
		props.setProperty("var1", "");
		props.setProperty("var2", "");
		props.setProperty("varFloat.aggregate.type", "avg");
		
		try {
			new Equation("x + y * z", props);
			fail();
		}catch(ParseException | ConfigurationException e) {
			assertEquals("Unknown variable: z", e.getMessage());
		}
		
		try {
			new Equation("functionUnknown(x)", props);
			fail();
		}catch(ParseException | ConfigurationException e) {
			assertEquals("Unknown function: functionUnknown", e.getMessage());
		}
		
		try {
			new Equation("4 + true", props);
			fail();
		}catch(ParseException | ConfigurationException e) {
			assertEquals("Function \"+\": expects type FloatValue for argument 2", e.getMessage());
		}
		
		try {
			new Equation("4 + 1 s", props);
			fail();
		}catch(ParseException | ConfigurationException e) {
			assertEquals("Unexpected: s", e.getMessage());
		}
		
		try {
			new Equation("4 + #1", props);
			fail();
		}catch(ParseException | ConfigurationException e) {
			assertEquals("Unexpected: #", e.getMessage());
		}
		
		try {
			new Equation("(x + 1) == trim(x)", props);
			fail();
		}catch(ParseException | ConfigurationException e) {
			assertEquals("Variable x is used by functions that expect it as different types (FloatValue, StringValue)", e.getMessage());
		}
		
		try {
			new Equation("trim(varFloat)", props);
			fail();
		}catch(ParseException | ConfigurationException e) {
			assertEquals("variable varFloat returns type FloatValue because of its aggregation operation, but in the equation there is a function that uses it as type StringValue", e.getMessage());
		}
	}
	
}
