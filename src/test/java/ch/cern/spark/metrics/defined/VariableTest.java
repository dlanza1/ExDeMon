package ch.cern.spark.metrics.defined;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class VariableTest {

	@Test
	public void countAggreagtion() throws ConfigurationException {
		Variable variable = new Variable("name");
		Properties properties = new Properties();
		properties.setProperty("aggregate", "count");
		variable.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();
		
		Instant now = Instant.now();
		
		store.updateAggregatedValue("name", 0, 0f, now);
		Optional<Double> result = variable.compute(store, Instant.now());
		assertTrue(result.isPresent());
		assertEquals(1, result.get(), 0f);
		
		store.updateAggregatedValue("name", 0, 0f, now.plus(Duration.ofSeconds(1)));
		result = variable.compute(store, now.plus(Duration.ofSeconds(1)));
		assertTrue(result.isPresent());
		assertEquals(2, result.get(), 0f);
		
		store.updateAggregatedValue("name", 0, 0f, now.plus(Duration.ofSeconds(2)));
		result = variable.compute(store, Instant.now());
		assertTrue(result.isPresent());
		assertEquals(3, result.get(), 0f);
	}
	
	@Test
	public void diffAggreagtion() throws ConfigurationException {
		Variable variable = new Variable("name");
		Properties properties = new Properties();
		properties.setProperty("aggregate", "diff");
		variable.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();
		
		Instant now = Instant.now();
		
		store.updateAggregatedValue("name", 0, 5f, now);
		Optional<Double> result = variable.compute(store, Instant.now());
		assertFalse(result.isPresent());
		
		store.updateAggregatedValue("name", 0, 10f, now.plus(Duration.ofSeconds(1)));
		result = variable.compute(store, now.plus(Duration.ofSeconds(1)));
		assertTrue(result.isPresent());
		assertEquals(5, result.get(), 0f);
		
		store.updateAggregatedValue("name", 0, 3f, now.plus(Duration.ofSeconds(2)));
		result = variable.compute(store, Instant.now());
		assertTrue(result.isPresent());
		assertEquals(-7, result.get(), 0f);
		
		store.updateAggregatedValue("name", 0, -10f, now.plus(Duration.ofSeconds(3)));
		result = variable.compute(store, Instant.now());
		assertTrue(result.isPresent());
		assertEquals(-13, result.get(), 0f);
	}
	
	@Test
	public void weightedAverageAggreagtion() throws ConfigurationException {
		Duration period = Duration.ofSeconds(60);
        Instant currentTime = Instant.now();
        
		Variable variable = new Variable("name");
		Properties properties = new Properties();
		properties.setProperty("aggregate", "weighted_avg");
		properties.setProperty("expire", period.getSeconds() + "");
		variable.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();
		
		Instant metric1_time = currentTime.minus(Duration.ofSeconds(40));
        float metric1_value = 10;
        float weight1 = (float) (period.toMillis() - Duration.between(currentTime, metric1_time).abs().toMillis()) / period.toMillis();
		store.updateAggregatedValue("name", 0, metric1_value, metric1_time);
		Optional<Double> result = variable.compute(store, currentTime);
		assertTrue(result.isPresent());
		float expectedValue = (metric1_value * weight1)  / (weight1);
		assertEquals(expectedValue, result.get(), 0f);
		
		Instant metric2_time = currentTime.minus(Duration.ofSeconds(20));
        float metric2_value = 20;
        float weight2 = (float) (period.toMillis() - Duration.between(currentTime, metric2_time).abs().toMillis()) / period.toMillis();
		store.updateAggregatedValue("name", 0, metric2_value, metric2_time);
		result = variable.compute(store, currentTime);
		assertTrue(result.isPresent());
		expectedValue = (metric1_value * weight1 + metric2_value * weight2)  / (weight1 + weight2);
		assertEquals(expectedValue, result.get(), 0.00001f);
		
		Instant metric3_time = currentTime.minus(Duration.ofSeconds(0));
        float metric3_value = 30;
        float weight3 = (float) (period.toMillis() - Duration.between(currentTime, metric3_time).abs().toMillis()) / period.toMillis();
		store.updateAggregatedValue("name", 0, metric3_value, metric3_time);
		result = variable.compute(store, currentTime);
		assertTrue(result.isPresent());
		
        expectedValue = (metric1_value * weight1 
                + metric2_value * weight2 
                + metric3_value * weight3) / (weight1 + weight2 + weight3);
        assertEquals(expectedValue, result.get(), 0.000001f);
	}
	
	@Test
	public void weightedAverageAggreagtionWithSameValue() throws ConfigurationException {
		int period = 50;
        
		Variable variable = new Variable("name");
		Properties properties = new Properties();
		properties.setProperty("aggregate", "weighted_avg");
		properties.setProperty("expire", period + "");
		variable.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();
		
		int metric1_time = 20;
        float metric1_value = 100;
		store.updateAggregatedValue("name", 0, metric1_value, Instant.ofEpochMilli(metric1_time));
		Optional<Double> result = variable.compute(store, Instant.ofEpochMilli(metric1_time));
		assertTrue(result.isPresent());
		assertEquals(100f, result.get(), 0f);
		
		int metric2_time = 30;
        float metric2_value = 100;
		store.updateAggregatedValue("name", 0, metric2_value, Instant.ofEpochMilli(metric2_time));
		result = variable.compute(store, Instant.ofEpochMilli(metric2_time));
		assertTrue(result.isPresent());
		assertEquals(100f, result.get(), 0f);
		
		int metric3_time = 40;
        float metric3_value = 100;
		store.updateAggregatedValue("name", 0, metric3_value, Instant.ofEpochMilli(metric3_time));
		result = variable.compute(store, Instant.ofEpochMilli(metric3_time));
		assertTrue(result.isPresent());
		assertEquals(100f, result.get(), 0f);
	}
	
}
