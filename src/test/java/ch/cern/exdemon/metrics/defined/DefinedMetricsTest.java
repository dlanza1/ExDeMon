package ch.cern.exdemon.metrics.defined;

import static ch.cern.exdemon.metrics.MetricTest.Metric;

import java.util.Optional;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.junit.Before;
import org.junit.Test;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.properties.Properties;
import ch.cern.spark.StreamTestHelper;

public class DefinedMetricsTest extends StreamTestHelper<Metric, Metric> {

	private static final long serialVersionUID = -2224810535022352025L;
	
	@Before
	public void reset() throws Exception {
		super.setUp();
		
		Properties properties = new Properties();
		properties.setProperty("type", "test");
        ComponentsCatalog.init(properties);
        ComponentsCatalog.reset();
	}

	@Test
	public void shouldGenerateMetrics() throws Exception {
        addInput(0,    Metric(1, 10f, "HOSTNAME=host1"));
        addInput(0,    Metric(1, 20f, "HOSTNAME=host2"));
        addExpected(0, Metric(1, 30f, "$defined_metric=dm1"));
        
        addInput(1,    Metric(2, 20f, "HOSTNAME=host1"));
        addInput(1,    Metric(2, 30f, "HOSTNAME=host2"));
        addExpected(1, Metric(2, 50f, "$defined_metric=dm1"));
		
        Properties properties = DefinedMetricTest.newProperties();
        properties.setProperty("variables.a.aggregate.type", "sum");
        properties.setProperty("variables.a.aggregate.attributes", "ALL");
        properties.setProperty("when", "batch");
        ComponentsCatalog.register(Type.METRIC, "dm1", properties);
	        
        JavaDStream<Metric> metricsStream = createStream(Metric.class);
        
        JavaDStream<Metric> results = DefinedMetrics.generate(metricsStream, null, Optional.empty());
        
        assertExpected(results);
	}
	
	@Test
	public void shouldApplyDefinedMetricFilter() throws Exception {
        addInput(0,    Metric(1, 10f, "CLUSTER=c1", "HOSTNAME=host1"));
        addInput(0,    Metric(1, 20f, "CLUSTER=c1", "HOSTNAME=host2"));
        addExpected(0, Metric(1, 30f, "$defined_metric=dm1"));
        
        addInput(1,    Metric(2, 7f,  "CLUSTER=c22", "HOSTNAME=host1"));
        addInput(1,    Metric(2, 13f, "CLUSTER=c22", "HOSTNAME=host2"));
        addExpected(1, Metric(2, 30f, "$defined_metric=dm1"));
        
        addInput(2,    Metric(3, 20f, "CLUSTER=c1", "HOSTNAME=host1"));
        addInput(2,    Metric(3, 30f, "CLUSTER=c1", "HOSTNAME=host2"));
        addExpected(2, Metric(3, 50f, "$defined_metric=dm1"));
        
        Properties properties = DefinedMetricTest.newProperties();
        properties.setProperty("metrics.filter.attribute.CLUSTER", "c1");
        properties.setProperty("variables.a.filter.attribute.HOSTNAME", ".*");
        properties.setProperty("variables.a.aggregate.type", "sum");
        properties.setProperty("variables.a.aggregate.attributes", "ALL");
        properties.setProperty("when", "batch");
        ComponentsCatalog.register(Type.METRIC, "dm1", properties);
	        
        JavaDStream<Metric> metricsStream = createStream(Metric.class);
        
        JavaDStream<Metric> results = DefinedMetrics.generate(metricsStream, null, Optional.empty());
        
        assertExpected(results);
	}
    
	@Test
	public void shouldGenerateMetricsWithComplexEquation() throws Exception {
        addInput(0,    Metric(1, true, 		"TYPE=DirReport", "$value_attribute=monitor_enable"));
        addInput(0,    Metric(1, " /tmp/  ", "TYPE=DirReport", "$value_attribute=path"));
        addInput(0,    Metric(1, 100, 		"TYPE=DirReport", "$value_attribute=used_bytes"));
        addInput(0,    Metric(1, 1000, 		"TYPE=DirReport", "$value_attribute=capacity_bytes"));
        // !true || ((trim("/tmp/") == "/tmp/") && (abs(100 / 1000) > 0.8))
        // false || ((true) && (0.1 > 0.8))
        	// false || ((true) && (false))
        // false || (false)
        // false
        addExpected(0, Metric(1, false, 		"$defined_metric=dm1"));
        
        addInput(1,    Metric(2, true, 		"TYPE=DirReport", "$value_attribute=monitor_enable"));
        addInput(1,    Metric(2, " /tmp/ ",  "TYPE=DirReport", "$value_attribute=path"));
        addInput(1,    Metric(2, 900, 		"TYPE=DirReport", "$value_attribute=used_bytes"));
        addInput(1,    Metric(2, 1000, 		"TYPE=DirReport", "$value_attribute=capacity_bytes"));
        // !true || ((trim("/tmp/") == "/tmp/") && (abs(900 / 1000) > 0.8))
        // false || ((true) && (0.9 > 0.8))
        	// false || ((true) && (true))
        // false || (true)
        // true
        addExpected(1, Metric(2, true, 		"$defined_metric=dm1"));
        
        Properties properties = DefinedMetricTest.newProperties();
        properties.setProperty("metrics.filter.attribute.TYPE", "DirReport");
        properties.setProperty("value", "!shouldBeMonitored || ((trim(dir) == \"/tmp/\") && (abs(used / capacity) > 0.8))");
        properties.setProperty("variables.shouldBeMonitored.filter.attribute.$value_attribute", "monitor_enable");
        properties.setProperty("variables.dir.filter.attribute.$value_attribute", "path");
        properties.setProperty("variables.used.filter.attribute.$value_attribute", "used_bytes");
        properties.setProperty("variables.capacity.filter.attribute.$value_attribute", "capacity_bytes");
        properties.setProperty("when", "batch");
        ComponentsCatalog.register(Type.METRIC, "dm1", properties);
	        
        JavaDStream<Metric> metricsStream = createStream(Metric.class);
        
        JavaDStream<Metric> results = DefinedMetrics.generate(metricsStream, null, Optional.empty());
        
        assertExpected(results);
	}
	
}
