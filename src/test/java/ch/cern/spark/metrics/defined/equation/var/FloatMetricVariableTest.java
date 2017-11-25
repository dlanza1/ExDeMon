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
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;

public class FloatMetricVariableTest {
	
	private FloatMetricVariable variable;
	private MetricVariableStore store;
	private Instant now = Instant.now();
	
	@Before
	public void setUp() {
		variable = new FloatMetricVariable("name");
		store = new MetricVariableStore();
	}
	
	@Test
	public void noAggregation() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		variable.config(properties);
		
		assertResult(10, 10f, 1);
		assertResult(20, 20f, 2);
		assertResult(-10.5f, -10.5f, 3);
	}

	@Test
	public void countAggreagtion() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		properties.setProperty("aggregate", "count_floats");
		variable.config(properties);
		
		assertResult(1, 0f, 1);
		assertResult(2, 0f, 2);
		assertResult(3, 0f, 3);
	}
	
	@Test
	public void sumAggreagtion() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		properties.setProperty("aggregate", "sum");
		variable.config(properties);
		
		assertResult(10, 10f, 1);
		assertResult(21, 11f, 2);
		assertResult(33, 12f, 3);
	}
	
	@Test
	public void avgAggreagtion() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		properties.setProperty("aggregate", "avg");
		variable.config(properties);
		
		assertResult(10, 10f, 1);
		assertResult(15, 20f, 2);
		assertResult(13.3333f, 10f, 3);
		assertResult(15f, 20f, 4);
	}
	
	@Test
	public void maxAggreagtion() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		properties.setProperty("aggregate", "max");
		variable.config(properties);
		
		assertResult(-10, -10f, 1);
		assertResult(-10, -20f, 2);
		assertResult(5f, 5f, 3);
		assertResult(5f, 0f, 4);
	}
	
	@Test(expected=ConfigurationException.class)
	public void unknownAggreagtion() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		properties.setProperty("aggregate", "unknown");
		variable.config(properties);
	}
	
	@Test
	public void minAggreagtion() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		properties.setProperty("aggregate", "min");
		variable.config(properties);
		
		assertResult(10, 10f, 1);
		assertResult(10, 20f, 2);
		assertResult(5f, 5f, 3);
		assertResult(-10f, -10f, 4);
	}

	@Test
	public void diffAggreagtion() throws ConfigurationException, ComputationException {
		Properties properties = new Properties();
		properties.setProperty("aggregate", "diff");
		variable.config(properties);
		
		store.updateAggregatedValue(0, 5f, now);
		Value result = variable.compute(store, Instant.now());
		assertTrue(result.getAsException().isPresent());

		assertResult(5, 10f, 1);
		assertResult(-7, 3f, 2);
		assertResult(-13, -10f, 3);
	}
	
	@Test
	public void weightedAverageAggreagtion() throws ConfigurationException, ComputationException {
		Duration period = Duration.ofSeconds(60);
        
		Properties properties = new Properties();
		properties.setProperty("aggregate", "weighted_avg");
		properties.setProperty("expire", period.getSeconds() + "");
		variable.config(properties);
		
		Instant metric1_time = now.minus(Duration.ofSeconds(40));
        float metric1_value = 10;
        float weight1 = (float) (period.toMillis() - Duration.between(now, metric1_time).abs().toMillis()) / period.toMillis();
        float expectedValue = (metric1_value * weight1)  / (weight1);
        assertResult(expectedValue, metric1_value, -40);
		
		Instant metric2_time = now.minus(Duration.ofSeconds(20));
        float metric2_value = 20;
        float weight2 = (float) (period.toMillis() - Duration.between(now, metric2_time).abs().toMillis()) / period.toMillis();
        expectedValue = (metric1_value * weight1 + metric2_value * weight2)  / (weight1 + weight2);
		assertResult(expectedValue, metric2_value, -20);
		
		Instant metric3_time = now.minus(Duration.ofSeconds(0));
        float metric3_value = 30;
        float weight3 = (float) (period.toMillis() - Duration.between(now, metric3_time).abs().toMillis()) / period.toMillis();
        expectedValue = (metric1_value * weight1 
                + metric2_value * weight2 
                + metric3_value * weight3) / (weight1 + weight2 + weight3);
        assertResult(expectedValue, metric3_value, 0);
	}
	
	@Test
	public void weightedAverageAggreagtionWithSameValue() throws ConfigurationException, ComputationException {
		int period = 50;
        
		Properties properties = new Properties();
		properties.setProperty("aggregate", "weighted_avg");
		properties.setProperty("expire", period + "");
		variable.config(properties);
		
		float metric_value = 100;
		
		assertResult(metric_value, metric_value, -20);
		assertResult(metric_value, metric_value, -10);
		assertResult(metric_value, metric_value, 0);
	}
	
	private void assertResult(float expected, float newValue, int nowPlusSeconds) {
		Map<String, String> ids = new HashMap<>();
		variable.updateStore(store, new Metric(
									now.plus(Duration.ofSeconds(nowPlusSeconds)), 
									new FloatValue(newValue), 
									ids));
		
		Value result = variable.compute(store, now);
		
		assertTrue(result.getAsFloat().isPresent());
		assertEquals(expected, result.getAsFloat().get(), 0.0001f);
	}
	
}
