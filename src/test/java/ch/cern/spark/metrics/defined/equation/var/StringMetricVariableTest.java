package ch.cern.spark.metrics.defined.equation.var;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.equation.ComputationException;
import ch.cern.spark.metrics.value.StringValue;
import ch.cern.spark.metrics.value.Value;

public class StringMetricVariableTest {
	
	private StringMetricVariable variable;
	private MetricVariableStatus store;
	private Instant now = Instant.now();
	
	@Before
	public void setUp() {
		variable = new StringMetricVariable("name");
		store = new MetricVariableStatus();
	}
	
	@Test
	public void noAggregation() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		variable.config(properties);
		
		assertResult("a", "a", 1);
		assertResult("b\"a", "b\"a", 2);
		assertResult("223", "223", 3);
	}

	@Test
	public void countAggreagtion() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		properties.setProperty("aggregate", "count_strings");
		variable.config(properties);
		
		assertResult(1, "a", 1);
		assertResult(2, "23", 2);
		assertResult(3, "  ", 3);
	}
	
	@Test(expected=ConfigurationException.class)
	public void unknownAggreagtion() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		properties.setProperty("aggregate", "unknown");
		variable.config(properties);
	}
	
	private void assertResult(String expected, String newValue, int nowPlusSeconds) {
		Map<String, String> ids = new HashMap<>();
		variable.updateStore(store, new Metric(
									now.plus(Duration.ofSeconds(nowPlusSeconds)), 
									new StringValue(newValue), 
									ids));
		
		Value result = variable.compute(store, now);
		
		assertTrue(result.getAsString().isPresent());
		assertEquals(expected, result.getAsString().get());
	}
	
	private void assertResult(float expected, String newValue, int nowPlusSeconds) {
		Map<String, String> ids = new HashMap<>();
		variable.updateStore(store, new Metric(
									now.plus(Duration.ofSeconds(nowPlusSeconds)), 
									new StringValue(newValue), 
									ids));
		
		Value result = variable.compute(store, now);
		
		assertTrue(result.getAsFloat().isPresent());
		assertEquals(expected, result.getAsFloat().get(), 0.00001f);
	}
	
}
