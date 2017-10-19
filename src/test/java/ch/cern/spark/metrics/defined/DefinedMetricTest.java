package ch.cern.spark.metrics.defined;

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

import org.junit.Test;

import ch.cern.ConfigurationException;
import ch.cern.Properties;
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
		assertEquals(new Equation("DBCPUUsagePerSec - HostCPUUsagePerSec"), metric.getEquation());
		assertNotNull(metric.getVariables().get("DBCPUUsagePerSec"));
		assertNotNull(metric.getVariables().get("HostCPUUsagePerSec"));
		assertEquals(new HashSet<String>(Arrays.asList("DBCPUUsagePerSec")), metric.getVariablesWhen());
		
		properties.setProperty("when", "DBCPUUsagePerSec, HostCPUUsagePerSec");
		metric.config(properties);
		assertEquals(new HashSet<String>(Arrays.asList("DBCPUUsagePerSec", "HostCPUUsagePerSec")), metric.getVariablesWhen());
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
		assertEquals(new Equation("DBCPUUsagePerSec - HostCPUUsagePerSec"), metric.getEquation());
		assertNotNull(metric.getVariables().get("DBCPUUsagePerSec"));
		assertNotNull(metric.getVariables().get("HostCPUUsagePerSec"));
		assertEquals(new HashSet<String>(Arrays.asList("DBCPUUsagePerSec")), metric.getVariablesWhen());
		
		properties.setProperty("when", "DBCPUUsagePerSec, HostCPUUsagePerSec");
		metric.config(properties);
		assertEquals(new HashSet<String>(Arrays.asList("DBCPUUsagePerSec", "HostCPUUsagePerSec")), metric.getVariablesWhen());
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

		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine");
		Metric metric = new Metric(null, 0f, ids);
		assertFalse(definedMetric.testIfApplyForAnyVariable(metric));
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine");
		ids.put("METRIC_NAME", "Host CPU Usage Per Sec");
		metric = new Metric(null, 0f, ids);
		assertTrue(definedMetric.testIfApplyForAnyVariable(metric));
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine");
		ids.put("METRIC_NAME", "Not included");
		metric = new Metric(null, 0f, ids);
		assertFalse(definedMetric.testIfApplyForAnyVariable(metric));
	}
	
	@Test
	public void testIfApplyForAnyVariableOneWithAggregation() throws ConfigurationException {
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("variables.DBCPUUsagePerSec.aggreagtion", "sum");
		definedMetric.config(properties);

		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine");
		Metric metric = new Metric(null, 0f, ids);
		assertTrue(definedMetric.testIfApplyForAnyVariable(metric));
	}
	
	@Test
	public void getGruopByMetricIDs() throws ConfigurationException {
		
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
	public void computeAggregateSumMetric() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
		properties.setProperty("variables.readbytestotal.aggregate", "sum");
		definedMetric.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();;
		
		Map<String, String> ids = new HashMap<>();
		ids.put("HOSTNAME", "host1");
		ids.put("METRIC_NAME", "Read Bytes");
		Metric metric = new Metric(Instant.now(), 10, ids);
		definedMetric.updateStore(store, metric);
		assertEquals(10f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		ids = new HashMap<>();
		ids.put("HOSTNAME", "host2");
		ids.put("METRIC_NAME", "Read Bytes");
		metric = new Metric(Instant.now(), 13, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(23f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		ids = new HashMap<>();
		ids.put("HOSTNAME", "host3");
		ids.put("METRIC_NAME", "Read Bytes");
		metric = new Metric(Instant.now(), 13, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(36f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		ids = new HashMap<>();
		ids.put("HOSTNAME", "host3"); // same host -> update value
		ids.put("METRIC_NAME", "Read Bytes");
		metric = new Metric(Instant.now(), 7, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(30f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
	}
	
	@Test
	public void neverExpire() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("value", "readbytestotal + writebytestotal");
		properties.setProperty("when", "ANY");
		properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
		properties.setProperty("variables.writebytestotal.filter.attribute.METRIC_NAME", "Write Bytes");
		properties.setProperty("variables.writebytestotal.expire", "never");
		definedMetric.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();;
		
		Instant now = Instant.now();
		
		Map<String, String> ids = new HashMap<>();
		ids.put("METRIC_NAME", "Read Bytes");
		Metric metric = new Metric(now, 10, ids);
		definedMetric.updateStore(store, metric);
		assertFalse(definedMetric.generate(store, metric, new HashMap<String, String>()).isPresent());
		
		ids = new HashMap<>();
		ids.put("METRIC_NAME", "Write Bytes");
		metric = new Metric(now.plus(Duration.ofHours(20)), 7, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(17f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		ids = new HashMap<>();
		ids.put("METRIC_NAME", "Write Bytes");
		metric = new Metric(now.plus(Duration.ofHours(40)), 8, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(18f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		ids = new HashMap<>();
		ids.put("METRIC_NAME", "Write Bytes");
		metric = new Metric(now.plus(Duration.ofHours(60)), 9, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(19f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		ids = new HashMap<>();
		ids.put("METRIC_NAME", "Write Bytes");
		metric = new Metric(now.plus(Duration.ofHours(80)), 10, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(20f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
	}

	@Test
	public void valueExpire() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("value", "readbytestotal + writebytestotal");
		properties.setProperty("when", "ANY");
		properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
		properties.setProperty("variables.writebytestotal.filter.attribute.METRIC_NAME", "Write Bytes");
		properties.setProperty("variables.writebytestotal.expire", "1m");
		definedMetric.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();;
		
		Instant now = Instant.now();
		
		Map<String, String> ids = new HashMap<>();
		ids.put("METRIC_NAME", "Read Bytes");
		Metric metric = new Metric(now, 10, ids);
		definedMetric.updateStore(store, metric);
		assertFalse(definedMetric.generate(store, metric, new HashMap<String, String>()).isPresent());
		
		ids = new HashMap<>();
		ids.put("METRIC_NAME", "Write Bytes");
		metric = new Metric(now.plus(Duration.ofSeconds(20)), 7, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(17f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		ids = new HashMap<>();
		ids.put("METRIC_NAME", "Write Bytes");
		metric = new Metric(now.plus(Duration.ofSeconds(40)), 8, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(18f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		ids = new HashMap<>();
		ids.put("METRIC_NAME", "Write Bytes");
		metric = new Metric(now.plus(Duration.ofSeconds(60)), 9, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(19f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		// Read Bytes has not been updated for more than 1 minute, so has expired and computation cannot be performed
		ids = new HashMap<>();
		ids.put("METRIC_NAME", "Write Bytes");
		metric = new Metric(now.plus(Duration.ofSeconds(80)), 8, ids );
		definedMetric.updateStore(store, metric);
		assertFalse(definedMetric.generate(store, metric, new HashMap<String, String>()).isPresent());
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
		
		Map<String, String> ids = new HashMap<>();
		ids.put("HOSTNAME", "host1");
		ids.put("METRIC_NAME", "Read Bytes");
		Metric metric = new Metric(now, 10, ids);
		definedMetric.updateStore(store, metric);
		assertEquals(10f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		ids = new HashMap<>();
		ids.put("HOSTNAME", "host2");
		ids.put("METRIC_NAME", "Read Bytes");
		metric = new Metric(now.plus(Duration.ofSeconds(20)), 13, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(23f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		ids = new HashMap<>();
		ids.put("HOSTNAME", "host3");
		ids.put("METRIC_NAME", "Read Bytes");
		metric = new Metric(now.plus(Duration.ofSeconds(40)), 13, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(36f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		ids = new HashMap<>();
		ids.put("HOSTNAME", "host3"); // same host -> update value
		ids.put("METRIC_NAME", "Read Bytes");
		metric = new Metric(now.plus(Duration.ofSeconds(60)), 7, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(30f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		// host1 has not been updated for more than 1 minute, so his value is removed
		ids = new HashMap<>();
		ids.put("HOSTNAME", "host2"); // same host -> update value
		ids.put("METRIC_NAME", "Read Bytes");
		metric = new Metric(now.plus(Duration.ofSeconds(80)), 8, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(15f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
	}
	
	@Test
	public void aggregateWithNoFilter() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("variables.readbytestotal.aggregate", "sum");
		definedMetric.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();;
		
		Map<String, String> ids = new HashMap<>();
		ids.put("HOSTNAME", "host1");
		Metric metric = new Metric(Instant.now(), 10, ids);
		definedMetric.updateStore(store, metric);
		assertEquals(10f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		ids = new HashMap<>();
		ids.put("HOSTNAME", "host2");
		metric = new Metric(Instant.now(), 13, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(23f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		ids = new HashMap<>();
		ids.put("HOSTNAME", "host3");
		metric = new Metric(Instant.now(), 13, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(36f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
		
		ids = new HashMap<>();
		ids.put("HOSTNAME", "host3"); // same host -> update value
		metric = new Metric(Instant.now(), 7, ids );
		definedMetric.updateStore(store, metric);
		assertEquals(30f, definedMetric.generate(store, metric, new HashMap<String, String>()).get().getValue(), 0.001f);
	}
	
	@Test
	public void filterInVariable() throws ConfigurationException {
		
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("variables.readbytestotal.filter.attribute.METRIC_NAME", "Read Bytes");
		definedMetric.config(properties);
		
		DefinedMetricStore store = new DefinedMetricStore();;
		
		Map<String, String> ids = new HashMap<>();
		ids.put("HOSTNAME", "host1");
		ids.put("METRIC_NAME", "Read Bytes");
		Metric metric = new Metric(Instant.now(), 10, ids);
		definedMetric.updateStore(store, metric);
		assertTrue(definedMetric.generate(store, metric, new HashMap<String, String>()).isPresent());
		
		ids = new HashMap<>();
		ids.put("HOSTNAME", "host2");
		ids.put("METRIC_NAME", "Read Bytes");
		metric = new Metric(Instant.now(), 14, ids );
		definedMetric.updateStore(store, metric);
		assertTrue(definedMetric.generate(store, metric, new HashMap<String, String>()).isPresent());
		
		ids = new HashMap<>();
		ids.put("HOSTNAME", "host3");
		ids.put("METRIC_NAME", "Write Bytes"); //filtered out
		metric = new Metric(Instant.now(), 15, ids );
		definedMetric.updateStore(store, metric);
		assertFalse(definedMetric.generate(store, metric, new HashMap<String, String>()).isPresent());
	}
	
}
