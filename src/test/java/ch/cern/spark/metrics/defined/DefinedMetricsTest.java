package ch.cern.spark.metrics.defined;

import static ch.cern.spark.metrics.MetricTest.Metric;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;

import java.time.Duration;
import java.util.Map;

import org.junit.Test;

import ch.cern.Cache;
import ch.cern.properties.Properties;
import ch.cern.spark.Stream;
import ch.cern.spark.StreamTestHelper;
import ch.cern.spark.metrics.Metric;

public class DefinedMetricsTest extends StreamTestHelper<Metric, Metric> {

	@Test
	public void shouldUpdateDefinedMetrics() throws Exception {
		DefinedMetrics.initCache(null);
		
		Cache<Map<String, DefinedMetric>> definedMetricsCache = DefinedMetrics.getCache();
				
		Properties.getCache().reset();
		definedMetricsCache.setExpiration(Duration.ofSeconds(1));
		
		//First load
		Map<String, DefinedMetric> original = definedMetricsCache.get();
		
		Map<String, DefinedMetric> returned = definedMetricsCache.get();
		assertSame(original, returned);
		
		Thread.sleep(Duration.ofMillis(100).toMillis());
		
		returned = definedMetricsCache.get();
		assertSame(original, returned);
		
		Thread.sleep(Duration.ofSeconds(1).toMillis());
		
		returned = definedMetricsCache.get();
		assertNotSame(original, returned);
	}

	@Test
	public void shouldGenerateMetrics() throws Exception {
		DefinedMetrics.getCache().reset();
		
        addInput(0,    Metric(1, 10f, "HOSTNAME=host1"));
        addInput(0,    Metric(1, 20f, "HOSTNAME=host2"));
        addExpected(0, Metric(1, 30f, "$defined_metric=dm1"));
        
        addInput(1,    Metric(2, 20f, "HOSTNAME=host1"));
        addInput(1,    Metric(2, 30f, "HOSTNAME=host2"));
        addExpected(1, Metric(2, 50f, "$defined_metric=dm1"));
		
        Cache<Properties> propertiesCache = Properties.getCache();
        propertiesCache.set(new Properties());
        propertiesCache.get().setProperty("metrics.define.dm1.variables.a.filter.attributes.HOSTNAME", ".*");
        propertiesCache.get().setProperty("metrics.define.dm1.variables.a.aggregate", "sum");
        propertiesCache.get().setProperty("metrics.define.dm1.when", "batch");
	        
        Stream<Metric> metricsStream = createStream(Metric.class);
        
		Stream<Metric> results = DefinedMetrics.generate(metricsStream, null);
        
        assertExpected(results);
	}
    
}
