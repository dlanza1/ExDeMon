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
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.Value;

public class BooleanMetricVariableTest {
	
	private BooleanMetricVariable variable;
	private MetricVariableStore store;
	private Instant now = Instant.now();
	
	@Before
	public void setUp() {
		variable = new BooleanMetricVariable("name");
		store = new MetricVariableStore();
	}
	
	@Test
	public void noAggregation() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		variable.config(properties);
		
		assertResult(true, true, 1);
		assertResult(false, false, 2);
		assertResult(true, true, 3);
	}

	@Test
	public void countAggreagtion() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		properties.setProperty("aggregate", "count_bools");
		variable.config(properties);
		
		assertResult(1, true, 1);
		assertResult(2, false, 2);
		assertResult(3, true, 3);
	}
	
	@Test
	public void countTrueAggreagtion() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		properties.setProperty("aggregate", "count_true");
		variable.config(properties);
		
		assertResult(1, true, 1);
		assertResult(1, false, 2);
		assertResult(2, true, 3);
	}
	
	@Test
	public void countFalseAggreagtion() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		properties.setProperty("aggregate", "count_false");
		variable.config(properties);
		
		assertResult(0, true, 1);
		assertResult(1, false, 2);
		assertResult(1, true, 3);
		assertResult(2, false, 3);
	}
	
	@Test(expected=ConfigurationException.class)
	public void unknownAggreagtion() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		properties.setProperty("aggregate", "unknown");
		variable.config(properties);
	}
	
	private void assertResult(boolean expected, boolean newValue, int nowPlusSeconds) {
		Map<String, String> ids = new HashMap<>();
		variable.updateStore(store, new Metric(
									now.plus(Duration.ofSeconds(nowPlusSeconds)), 
									new BooleanValue(newValue), 
									ids));
		
		Value result = variable.compute(store, now);
		
		assertTrue(result.getAsBoolean().isPresent());
		assertEquals(expected, result.getAsBoolean().get());
	}
	
	private void assertResult(float expected, boolean newValue, int nowPlusSeconds) {
		Map<String, String> ids = new HashMap<>();
		variable.updateStore(store, new Metric(
									now.plus(Duration.ofSeconds(nowPlusSeconds)), 
									new BooleanValue(newValue), 
									ids));
		
		Value result = variable.compute(store, now);
		
		assertTrue(result.getAsFloat().isPresent());
		assertEquals(expected, result.getAsFloat().get(), 0.00001f);
	}
	
}
