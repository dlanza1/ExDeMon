package ch.cern.spark.metrics.defined;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.junit.Test;

import ch.cern.Properties;
import ch.cern.spark.metrics.Metric;
import scala.Tuple2;

public class ComputeIDsForDefinedMetricsFTest {

	@Test
	public void oneMetricsIsMultuplyedBySeveralDefinedMetrics() throws Exception {
		ComputeIDsForDefinedMetricsF function = getTwoDefinedMetricsWithOneGroupByAttribute();
		
		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine1");
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		Metric metric = new Metric(null, 0f, ids);
		
		Iterator<Tuple2<DefinedMetricID, Metric>> result;
		
		result = function.call(metric);
		assertTrue(result.hasNext());
		Tuple2<DefinedMetricID, Metric> tuple = result.next();
		assertEquals("defM1", tuple._1.getDefinedMetricName());
		assertEquals(1, tuple._1.getGroupByMetricIDs().size());
		assertEquals("prod-machine1", tuple._1.getGroupByMetricIDs().get("INSTANCE_NAME"));
		assertSame(metric, tuple._2);
		tuple = result.next();
		assertEquals("defM2", tuple._1.getDefinedMetricName());
		assertEquals(1, tuple._1.getGroupByMetricIDs().size());
		assertEquals("prod-machine1", tuple._1.getGroupByMetricIDs().get("INSTANCE_NAME"));
		assertSame(metric, tuple._2);
		assertFalse(result.hasNext());
		
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine2");
		ids.put("METRIC_NAME", "Host CPU Usage Per Sec");
		metric = new Metric(null, 0f, ids);
		
		result = function.call(metric);
		assertTrue(result.hasNext());
		tuple = result.next();
		assertEquals("defM1", tuple._1.getDefinedMetricName());
		assertEquals(1, tuple._1.getGroupByMetricIDs().size());
		assertEquals("prod-machine2", tuple._1.getGroupByMetricIDs().get("INSTANCE_NAME"));
		assertSame(metric, tuple._2);
		tuple = result.next();
		assertEquals("defM2", tuple._1.getDefinedMetricName());
		assertEquals(1, tuple._1.getGroupByMetricIDs().size());
		assertEquals("prod-machine2", tuple._1.getGroupByMetricIDs().get("INSTANCE_NAME"));
		assertSame(metric, tuple._2);
		assertFalse(result.hasNext());
	}
	
	@Test
	public void metricDoNotPassFilters() throws Exception {
		ComputeIDsForDefinedMetricsF function = getTwoDefinedMetricsWithOneGroupByAttribute();
		
		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine1");
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		Metric metric = new Metric(null, 0f, ids);
		
		Iterator<Tuple2<DefinedMetricID, Metric>> result;
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine2");
		ids.put("METRIC_NAME", "To be filtered out");
		metric = new Metric(null, 0f, ids);
		
		result = function.call(metric);
		assertFalse(result.hasNext());
		
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "to be filter out by defM1");
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		metric = new Metric(null, 0f, ids);
		
		result = function.call(metric);
		assertTrue(result.hasNext());
		Tuple2<DefinedMetricID, Metric> tuple = result.next();
		assertEquals("defM2", tuple._1.getDefinedMetricName());
		assertFalse(result.hasNext());
	}
	
	@Test
	public void groupByAll() throws Exception {
		ComputeIDsForDefinedMetricsF function = getGroupByAll();
		
		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine1");
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		ids.put("METRIC_NAME_SHORT", "CPUUsage");
		Metric metric = new Metric(null, 0f, ids);
		
		Iterator<Tuple2<DefinedMetricID, Metric>> result = function.call(metric);
		assertTrue(result.hasNext());
		Tuple2<DefinedMetricID, Metric> tuple = result.next();
		assertEquals("defMAll", tuple._1.getDefinedMetricName());
		assertEquals(3, tuple._1.getGroupByMetricIDs().size());
		assertEquals("prod-machine1", tuple._1.getGroupByMetricIDs().get("INSTANCE_NAME"));
		assertEquals("CPU Usage Per Sec", tuple._1.getGroupByMetricIDs().get("METRIC_NAME"));
		assertEquals("CPUUsage", tuple._1.getGroupByMetricIDs().get("METRIC_NAME_SHORT"));
		assertFalse(result.hasNext());
	}
	
	@Test
	public void metricMissingGrouByAttributeShouldNotPass() throws Exception {
		ComputeIDsForDefinedMetricsF function = getTwoDefinedMetricsWithOneGroupByAttribute();
		
		Map<String, String> ids = new HashMap<>();
//		ids.put("INSTANCE_NAME", null); <- INSTANCE_NAME is in groupby
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		Metric metric = new Metric(null, 0f, ids);
		
		Iterator<Tuple2<DefinedMetricID, Metric>> result = function.call(metric);
		assertFalse(result.hasNext());
	}
	
	@Test
	public void groupByAllDoNotPassAnyFIlter() throws Exception {
		ComputeIDsForDefinedMetricsF function = getGroupByAll();
		
		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "dev-machine1"); //to be filtered out
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		ids.put("METRIC_NAME_SHORT", "CPUUsage"); //is not filter anywhere
		Metric metric = new Metric(null, 0f, ids);
		
		Iterator<Tuple2<DefinedMetricID, Metric>> result = function.call(metric);
		assertFalse(result.hasNext());
		
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine1"); 
		ids.put("METRIC_NAME", "Not listed metric name"); //to be filtered out
		ids.put("METRIC_NAME_SHORT", "CPUUsage");  //is not filter anywhere
		metric = new Metric(null, 0f, ids);
		
		result = function.call(metric);
		assertFalse(result.hasNext());
	}
	
	@Test
	public void groupByNone() throws Exception {
		ComputeIDsForDefinedMetricsF function = getGroupByNone();
		
		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine1");
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		ids.put("METRIC_NAME_SHORT", "CPUUsage");
		Metric metric = new Metric(null, 0f, ids);
		
		Iterator<Tuple2<DefinedMetricID, Metric>> result = function.call(metric);
		assertTrue(result.hasNext());
		Tuple2<DefinedMetricID, Metric> tuple = result.next();
		assertEquals("defMNone", tuple._1.getDefinedMetricName());
		assertEquals(0, tuple._1.getGroupByMetricIDs().size());
		assertFalse(result.hasNext());
	}
	
	@Test
	public void groupByNoneDoNotPassAnyFIlter() throws Exception {
		ComputeIDsForDefinedMetricsF function = getGroupByAll();
		
		Map<String, String> ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "dev-machine1"); //to be filtered out
		ids.put("METRIC_NAME", "CPU Usage Per Sec");
		ids.put("METRIC_NAME_SHORT", "CPUUsage"); //is not filter anywhere
		Metric metric = new Metric(null, 0f, ids);
		
		Iterator<Tuple2<DefinedMetricID, Metric>> result = function.call(metric);
		assertFalse(result.hasNext());
		
		
		ids = new HashMap<>();
		ids.put("INSTANCE_NAME", "prod-machine1"); 
		ids.put("METRIC_NAME", "Not listed metric name"); //to be filtered out
		ids.put("METRIC_NAME_SHORT", "CPUUsage");  //is not filter anywhere
		metric = new Metric(null, 0f, ids);
		
		result = function.call(metric);
		assertFalse(result.hasNext());
	}

	private ComputeIDsForDefinedMetricsF getTwoDefinedMetricsWithOneGroupByAttribute() throws Exception {
		DefinedMetrics definedMetricsCache = DefinedMetricsTest.mockedExpirable();
		definedMetricsCache.get().put("defM1", new DefinedMetric("defM1"));
		Properties properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("metrics.groupby", "INSTANCE_NAME");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "regex:prod-.*");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "regex:prod-.*");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		definedMetricsCache.get().get("defM1").config(properties);
		
		definedMetricsCache.get().put("defM2", new DefinedMetric("defM2"));
		properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("metrics.groupby", "INSTANCE_NAME");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		definedMetricsCache.get().get("defM2").config(properties);
		
		return new ComputeIDsForDefinedMetricsF(definedMetricsCache);
	}
	
	private ComputeIDsForDefinedMetricsF getGroupByAll() throws Exception {
		DefinedMetrics definedMetricsCache = DefinedMetricsTest.mockedExpirable();
		definedMetricsCache.get().put("defMAll", new DefinedMetric("defMAll"));
		Properties properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
		properties.setProperty("metrics.groupby", "ALL");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "regex:prod-.*");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "regex:prod-.*");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		definedMetricsCache.get().get("defMAll").config(properties);
		
		return new ComputeIDsForDefinedMetricsF(definedMetricsCache);
	}
	
	private ComputeIDsForDefinedMetricsF getGroupByNone() throws Exception {
		DefinedMetrics definedMetricsCache = DefinedMetricsTest.mockedExpirable();
		definedMetricsCache.get().put("defMNone", new DefinedMetric("defMNone"));
		Properties properties = new Properties();
		properties.setProperty("value", "DBCPUUsagePerSec - HostCPUUsagePerSec");
//		properties.setProperty("metric.groupby", null);
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "regex:prod-.*");
		properties.setProperty("variables.DBCPUUsagePerSec.filter.attribute.METRIC_NAME", "CPU Usage Per Sec");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.INSTANCE_NAME", "regex:prod-.*");
		properties.setProperty("variables.HostCPUUsagePerSec.filter.attribute.METRIC_NAME", "Host CPU Usage Per Sec");
		definedMetricsCache.get().get("defMNone").config(properties);
		
		return new ComputeIDsForDefinedMetricsF(definedMetricsCache);
	}
	
}
