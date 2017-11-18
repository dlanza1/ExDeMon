package ch.cern.spark.metrics.defined;

import static ch.cern.spark.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;

public class DefinedMetricTest {
	
	@Test
	public void configNotValid() {
		
		Properties props = new Properties();
		
		//Value must be specified.
		props.setProperty("metric.y.filter.attribute.AA", "metricAA");
		DefinedMetric metric = new DefinedMetric(null);
		try{
			metric.config(props);
			fail();
		}catch(ConfigurationException e) {}
		
		//At least a metric must be described.
		props = new Properties();
		props.setProperty("value", "x * 10");
		metric = new DefinedMetric(null);
		try{
			metric.config(props);
			fail();
		}catch(ConfigurationException e) {}
		
		//Equation contain variables that have not been described.
		props = new Properties();
		props.setProperty("value", "x * 10");
		props.setProperty("metric.y.filter.attribute.AA", "metricAA");
		metric = new DefinedMetric(null);
		try{
			metric.config(props);
			fail();
		}catch(ConfigurationException e) {}
		
		//Metrics listed in when parameter must be declared.
		props = new Properties();
		props.setProperty("value", "x * 10");
		props.setProperty("when", "y");
		props.setProperty("metric.x.filter.attribute.AA", "metricAA");
		metric = new DefinedMetric(null);
		try{
			metric.config(props);
			fail();
		}catch(ConfigurationException e) {}
	}

	@Test
	public void config() throws ConfigurationException {
		DefinedMetric metric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		metric.config(properties);
		
		assertEquals("A", metric.getName());
		assertNotNull(metric.getEquation());
		assertNotNull(metric.getVariables().get("DBCPUUsagePerSec"));
		assertNotNull(metric.getVariables().get("HostCPUUsagePerSec"));
		assertEquals(new HashSet<String>(Arrays.asList("DBCPUUsagePerSec")), metric.getVariablesWhen());
		
		properties.setProperty("when", "DBCPUUsagePerSec HostCPUUsagePerSec");
		metric.config(properties);
		assertEquals(new HashSet<String>(Arrays.asList("HostCPUUsagePerSec", "DBCPUUsagePerSec")), metric.getVariablesWhen());
	}
	
	@Test
	public void configAggregaate() throws ConfigurationException {
		DefinedMetric metric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.DBCPUUsagePerSec.aggregate.attribute", "METRIC_NAME");
		properties.setProperty("variables.DBCPUUsagePerSec.aggregate.operation", "+");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		metric.config(properties);
		
		assertEquals("A", metric.getName());
		assertNotNull(metric.getEquation());
		assertNotNull(metric.getVariables().get("DBCPUUsagePerSec"));
		assertNotNull(metric.getVariables().get("HostCPUUsagePerSec"));
		assertEquals(new HashSet<String>(Arrays.asList("DBCPUUsagePerSec")), metric.getVariablesWhen());
		
		properties.setProperty("when", "DBCPUUsagePerSec HostCPUUsagePerSec");
		metric.config(properties);
		assertEquals(new HashSet<String>(Arrays.asList("HostCPUUsagePerSec", "DBCPUUsagePerSec")), metric.getVariablesWhen());
	}
	
	@Test
	public void testIfApplyForAnyVariable() throws ConfigurationException {
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.INSTANCE_NAME", ".*");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		definedMetric.config(properties);

		Metric metric = Metric(0, 0f, "INSTANCE_NAME=machine");
		assertFalse(definedMetric.testIfApplyForAnyVariable(metric));
		
		metric = Metric(0, 0f, "INSTANCE_NAME=machine", "METRIC_NAME=Host CPU Usage Per Sec");
		assertTrue(definedMetric.testIfApplyForAnyVariable(metric));
		
		metric = Metric(0, 0f, "INSTANCE_NAME=machine", "METRIC_NAME=Not included");
		assertFalse(definedMetric.testIfApplyForAnyVariable(metric));
	}
	
	@Test
	public void testIfApplyForAnyVariableOneWithAggregation() throws ConfigurationException {
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("variables.DBCPUUsagePerSec.aggreagtion", "sum");
		definedMetric.config(properties);

		Metric metric = Metric(0, 0f, "INSTANCE_NAME=machine");
		assertTrue(definedMetric.testIfApplyForAnyVariable(metric));
	}
	
	@Test
	public void getGroupByMetricIDs() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		definedMetric.config(properties);

		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(0, definedMetric.getGroupByMetricIDs(ids).get().size());
		
		
		properties.setProperty("metrics.groupby", "ALL");
		definedMetric.config(properties);
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(2, definedMetric.getGroupByMetricIDs(ids).get().size());
		assertEquals("machine1", definedMetric.getGroupByMetricIDs(ids).get().get("INSTANCE_NAME"));
		assertEquals("metric1", definedMetric.getGroupByMetricIDs(ids).get().get("METRIC_NAME"));
		
		
		properties.setProperty("metrics.groupby", "INSTANCE_NAME");
		definedMetric.config(properties);
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(1, definedMetric.getGroupByMetricIDs(ids).get().size());
		assertEquals("machine1", definedMetric.getGroupByMetricIDs(ids).get().get("INSTANCE_NAME"));
		
		
		properties.setProperty("metrics.groupby", "INSTANCE_NAME, METRIC_NAME");
		definedMetric.config(properties);
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(2, definedMetric.getGroupByMetricIDs(ids).get().size());
		assertEquals("machine1", definedMetric.getGroupByMetricIDs(ids).get().get("INSTANCE_NAME"));
		assertEquals("metric1", definedMetric.getGroupByMetricIDs(ids).get().get("METRIC_NAME"));
		
		
		properties.setProperty("metrics.groupby", "INSTANCE_NAME, METRIC_NAME");
		definedMetric.config(properties);
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(2, definedMetric.getGroupByMetricIDs(ids).get().size());
		assertEquals("machine1", definedMetric.getGroupByMetricIDs(ids).get().get("INSTANCE_NAME"));
		assertEquals("metric1", definedMetric.getGroupByMetricIDs(ids).get().get("METRIC_NAME"));
	}
	
	@Test
	public void computeWhenVariableThatIsNotInEqaution() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("value", "running_count");
		properties.setProperty("when", "trigger");
		properties.setProperty("variables.running_count.filter.attribute.TYPE", "Running");
		properties.setProperty("variables.running_count.aggregate", "count_floats");
		properties.setProperty("variables.running_count.expire", "10m");
		properties.setProperty("variables.trigger.filter.attribute.TYPE", "Trigger");
		definedMetric.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();
		
		Instant now = Instant.now();
		
		Metric metric = Metric(now, 10f, "HOSTNAME=host1", "TYPE=Running");
		definedMetric.updateStore(store, metric);
		assertFalse(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		
		metric = Metric(now.plus(Duration.ofMinutes(1)), 13, "HOSTNAME=host2", "TYPE=Running");
		definedMetric.updateStore(store, metric);
		assertFalse(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		
		metric = Metric(now.plus(Duration.ofMinutes(2)), 13, "TYPE=Trigger");
		definedMetric.updateStore(store, metric);
		Optional<Metric> result = definedMetric.generateByUpdate(store, metric, new HashMap<String, String>());
		assertTrue(result.isPresent());
		assertEquals(2f, result.get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofMinutes(3)), 7, "HOSTNAME=host3", "TYPE=Running");
		definedMetric.updateStore(store, metric);
		assertFalse(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		
		metric = Metric(now.plus(Duration.ofMinutes(4)), 13, "TYPE=Trigger");
		definedMetric.updateStore(store, metric);
		result = definedMetric.generateByUpdate(store, metric, new HashMap<String, String>());
		assertTrue(result.isPresent());
		assertEquals(3f, result.get().getValue().getAsFloat().get(), 0.001f);
	}
	
	@Test
	public void computeAggregateCountWhenBatch() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("when", "batch");
		properties.setProperty("variables.running_count.filter.attribute.TYPE", "Running");
		properties.setProperty("variables.running_count.aggregate", "count_floats");
		properties.setProperty("variables.running_count.expire", "10m");
		definedMetric.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();
		
		Instant now = Instant.now();
		
		Metric metric = Metric(now, 10f, "HOSTNAME=host1", "TYPE=Running");
		definedMetric.updateStore(store, metric);
		assertFalse(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		
		metric = Metric(now.plus(Duration.ofMinutes(1)), 13, "HOSTNAME=host2", "TYPE=Running");
		definedMetric.updateStore(store, metric);
		assertEquals(2f, definedMetric.generateByBatch(store, metric.getInstant(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofMinutes(2)), 13, "HOSTNAME=host3", "TYPE=Running");
		definedMetric.updateStore(store, metric);
		assertEquals(3f, definedMetric.generateByBatch(store, metric.getInstant(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		// same host -> update value
		metric = Metric(now.plus(Duration.ofMinutes(3)), 7, "HOSTNAME=host3", "TYPE=Running");
		definedMetric.updateStore(store, metric);
		assertEquals(3f, definedMetric.generateByBatch(store, metric.getInstant(), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		assertEquals(3f, definedMetric.generateByBatch(store, now.plus(Duration.ofMinutes(9)), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		assertEquals(3f, definedMetric.generateByBatch(store, now.plus(Duration.ofMinutes(10)), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		assertEquals(2f, definedMetric.generateByBatch(store, now.plus(Duration.ofMinutes(11)), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		assertEquals(1f, definedMetric.generateByBatch(store, now.plus(Duration.ofMinutes(12)), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		assertEquals(1f, definedMetric.generateByBatch(store, now.plus(Duration.ofMinutes(13)), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		assertEquals(0f, definedMetric.generateByBatch(store, now.plus(Duration.ofMinutes(14)), new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
	}
	
	@Test
	public void computeAggregateSumMetric() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("variables.readbytestotal.filter.attribute.TYPE", "Read Bytes");
		properties.setProperty("variables.readbytestotal.aggregate", "sum");
		definedMetric.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();;
		
		Metric metric = Metric(Instant.now(), 10, "HOSTNAME=host1", "TYPE=Read Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(10f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(Instant.now(), 13, "HOSTNAME=host2", "TYPE=Read Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(23f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(Instant.now(), 13, "HOSTNAME=host3", "TYPE=Read Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(36f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		// same host -> update value
		metric = Metric(Instant.now(), 7, "HOSTNAME=host3", "TYPE=Read Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(30f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
	}
	
	@Test
	public void neverExpire() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("value", "readbytestotal + writebytestotal");
		properties.setProperty("when", "ANY");
		properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
		properties.setProperty("variables.readbytestotal.expire", "never");
		properties.setProperty("variables.writebytestotal.filter.attribute.METRIC_NAME", "Write Bytes");
		definedMetric.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();;
		
		Instant now = Instant.now();
		
		Metric metric = Metric(now, 10, "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(store, metric);
		assertTrue(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		assertTrue(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsException().isPresent());
		
		metric = Metric(now.plus(Duration.ofHours(20)), 7, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(17f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofHours(40)), 8, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(18f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofHours(60)), 9, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(19f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofHours(80)), 10, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(20f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
	}

	@Test
	public void valueExpire() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("value", "readbytestotal + writebytestotal");
		properties.setProperty("when", "ANY");
		properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
		properties.setProperty("variables.readbytestotal.expire", "1m");
		properties.setProperty("variables.writebytestotal.filter.attribute.METRIC_NAME", "Write Bytes");
		properties.setProperty("variables.trigger.filter.attribute.METRIC_NAME", ".*");
		definedMetric.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();;
		
		Instant now = Instant.now();
	
		Metric metric = Metric(now, 10, "METRIC_NAME=None");
		definedMetric.updateStore(store, metric);
		Optional<Metric> result = definedMetric.generateByUpdate(store, metric, new HashMap<String, String>());
		assertTrue(result.isPresent());
		assertEquals("Variable readbytestotal: no value for the last 1 minute, Variable writebytestotal: no value for the last 10 minutes", result.get().getValue().getAsException().get());
		assertEquals("(var(readbytestotal)={Error: no value for the last 1 minute} + var(writebytestotal)={Error: no value for the last 10 minutes})={Error: in arguments}", result.get().getValue().getSource());
		
		metric = Metric(now, 10, "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(store, metric);
		result = definedMetric.generateByUpdate(store, metric, new HashMap<String, String>());
		assertTrue(result.isPresent());
		assertEquals("Variable writebytestotal: no value for the last 10 minutes", result.get().getValue().getAsException().get());
		assertEquals("(var(readbytestotal)=10.0 + var(writebytestotal)={Error: no value for the last 10 minutes})={Error: in arguments}", result.get().getValue().getSource());
		
		metric = Metric(now.plus(Duration.ofSeconds(20)), 7, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(17f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofSeconds(40)), 8, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(18f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofSeconds(60)), 9, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(19f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		// Read Bytes has not been updated for more than 1 minute, so has expired and computation cannot be performed
		metric = Metric(now.plus(Duration.ofSeconds(80)), 8, "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(store, metric);
		assertTrue(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		assertTrue(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsException().isPresent());
	}
	
	@Test
	public void valueExpireInAggregation() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
		properties.setProperty("variables.readbytestotal.aggregate", "sum");
		properties.setProperty("variables.readbytestotal.expire", "1m");
		definedMetric.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();;
		
		Instant now = Instant.now();
		
		Metric metric = Metric(now, 10, "HOSTNAME=host1", "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(10f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofSeconds(20)), 13, "HOSTNAME=host2", "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(23f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(now.plus(Duration.ofSeconds(40)), 13, "HOSTNAME=host3", "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(36f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		// same host -> update value
		metric = Metric(now.plus(Duration.ofSeconds(60)), 7, "HOSTNAME=host3", "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(30f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		// host1 has not been updated for more than 1 minute, so his value is removed
		// same host -> update value
		metric = Metric(now.plus(Duration.ofSeconds(80)), 8, "HOSTNAME=host2", "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(store, metric);
		assertEquals(15f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
	}
	
	@Test
	public void aggregateWithNoFilter() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("variables.readbytestotal.aggregate", "sum");
		definedMetric.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();;
		
		Metric metric = Metric(0, 10, "HOSTNAME=host1");
		definedMetric.updateStore(store, metric);
		assertEquals(10f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(0, 13, "HOSTNAME=host2");
		definedMetric.updateStore(store, metric);
		assertEquals(23f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		metric = Metric(0, 13, "HOSTNAME=host3");
		definedMetric.updateStore(store, metric);
		assertEquals(36f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
		
		// same host -> update value
		metric = Metric(0, 7, "HOSTNAME=host3");
		definedMetric.updateStore(store, metric);
		assertEquals(30f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
	}
	
	@Test
	public void filterInVariable() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
		definedMetric.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();;
		
		Metric metric = Metric(0, 10, "HOSTNAME=host1", "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(store, metric);
		assertTrue(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		
		metric = Metric(Instant.now(), 14, "HOSTNAME=host2", "METRIC_NAME=Read Bytes");
		definedMetric.updateStore(store, metric);
		assertTrue(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
		
		//filtered out
		metric = Metric(Instant.now(), 15, "HOSTNAME=host3", "METRIC_NAME=Write Bytes");
		definedMetric.updateStore(store, metric);
		assertFalse(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
	}
	
}
