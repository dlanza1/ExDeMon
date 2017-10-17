package ch.cern.spark.metrics.defined;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.configuration.ConfigurationException;
import org.junit.Test;

import ch.cern.Properties;
import ch.cern.spark.metrics.Metric;

public class DefinedMetricTest {

	@Test
	public void config() throws ConfigurationException {
		DefinedMetric metric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("metric.DBCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "regex:.*");
		properties.setProperty("metric.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("metric.HostCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "regex:.*");
		properties.setProperty("metric.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		metric.config(properties);
		
		assertEquals("A", metric.getName());
		assertEquals(new Equation("DBCPUUsagePerSec - HostCPUUsagePerSec"), metric.getEquation());
		assertNotNull(metric.getMetricsAndFilters().get("DBCPUUsagePerSec"));
		assertNotNull(metric.getMetricsAndFilters().get("HostCPUUsagePerSec"));
		assertEquals(new HashSet<String>(Arrays.asList("DBCPUUsagePerSec")), metric.getMetricsWhen());
		
		properties.setProperty("when", "DBCPUUsagePerSec, HostCPUUsagePerSec");
		metric.config(properties);
		assertEquals(new HashSet<String>(Arrays.asList("DBCPUUsagePerSec", "HostCPUUsagePerSec")), metric.getMetricsWhen());
	}
	
	@Test
	public void testIfAny() throws ConfigurationException {
		DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("metric.DBCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "regex:.*");
		properties.setProperty("metric.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("metric.HostCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "regex:.*");
		properties.setProperty("metric.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		definedMetric.config(properties);

		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine");
		Metric metric = new Metric(null, 0f, ids);
		assertFalse(definedMetric.testIfAnyFilter(metric));
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine");
		ids.put("METRIC_NAME", "Host CPU Usage Per Sec");
		metric = new Metric(null, 0f, ids);
		assertTrue(definedMetric.testIfAnyFilter(metric));
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine");
		ids.put("METRIC_NAME", "Not included");
		metric = new Metric(null, 0f, ids);
		assertFalse(definedMetric.testIfAnyFilter(metric));
	}
	
	@Test
	public void getGruopByMetricIDs() throws ConfigurationException {
		
DefinedMetric definedMetric = new DefinedMetric("A");
		
		Properties properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("metric.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("metric.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		definedMetric.config(properties);

		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(0, definedMetric.getGruopByMetricIDs(ids).size());
		
		
		properties.setProperty("metric.groupby", "ALL");
		definedMetric.config(properties);
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(2, definedMetric.getGruopByMetricIDs(ids).size());
		assertEquals("machine1", definedMetric.getGruopByMetricIDs(ids).get("INSTANCE_NAME"));
		assertEquals("metric1", definedMetric.getGruopByMetricIDs(ids).get("METRIC_NAME"));
		
		
		properties.setProperty("metric.groupby", "INSTANCE_NAME");
		definedMetric.config(properties);
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(1, definedMetric.getGruopByMetricIDs(ids).size());
		assertEquals("machine1", definedMetric.getGruopByMetricIDs(ids).get("INSTANCE_NAME"));
		
		
		properties.setProperty("metric.groupby", "INSTANCE_NAME, METRIC_NAME, not-valid-string");
		definedMetric.config(properties);
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(2, definedMetric.getGruopByMetricIDs(ids).size());
		assertEquals("machine1", definedMetric.getGruopByMetricIDs(ids).get("INSTANCE_NAME"));
		assertEquals("metric1", definedMetric.getGruopByMetricIDs(ids).get("METRIC_NAME"));
		
		
		properties.setProperty("metric.groupby", "INSTANCE_NAME, METRIC_NAME");
		definedMetric.config(properties);
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "machine1");
		ids.put("METRIC_NAME", "metric1");
		assertEquals(2, definedMetric.getGruopByMetricIDs(ids).size());
		assertEquals("machine1", definedMetric.getGruopByMetricIDs(ids).get("INSTANCE_NAME"));
		assertEquals("metric1", definedMetric.getGruopByMetricIDs(ids).get("METRIC_NAME"));
	}
	
}
