package ch.cern.spark.metrics.monitors;

import static ch.cern.spark.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.Test;

import ch.cern.Cache;
import ch.cern.properties.Properties;
import ch.cern.spark.Batches;
import ch.cern.spark.Stream;
import ch.cern.spark.StreamTestHelper;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;

public class MonitorReturningAnalysisResultsStreamTest extends StreamTestHelper<Metric, AnalysisResult> {
	
	private static final long serialVersionUID = -444431845152738589L;

	@Test
	public void shouldProduceExceptionAnaylisisResultWithConfigurationExceptionAndFilterOK() throws Exception {
		Monitors.getCache().reset();		
        Properties.initCache(null);
        Cache<Properties> propertiesCache = Properties.getCache();
        Properties properties = new Properties();
		properties.setProperty("monitor.mon1.filter.expr", "HOST=host1");
		properties.setProperty("monitor.mon1.analysis.type", "does not exist");
		properties.setProperty("monitor.mon1.analysis.error.upperbound", "20");
		properties.setProperty("monitor.mon1.analysis.error.lowerbound", "10");
        propertiesCache.set(properties);
	        
        addInput(0,    Metric(0, 0, "HOST=host1"));
        addInput(0,    Metric(20, 0, "HOST=host2345"));
        addInput(0,    Metric(40, 0, "HOST=host1"));
        
        addInput(1,    Metric(60, 0, "HOST=host243"));
        addInput(1,    Metric(80, 0));
        
        addInput(2,    Metric(60, 0, "HOST=host243"));
        addInput(2,    Metric(80, 0));
        
        addInput(3,    Metric(100, 0, "HOST=host1"));
        addInput(3,    Metric(120, 0, "HOST=host1"));
        addInput(3,    Metric(140, 0));
        Stream<Metric> metricsStream = createStream(Metric.class);
        
		Stream<AnalysisResult> results = Monitors.analyze(metricsStream, null);
        
        Batches<AnalysisResult> returnedBatches = collect(results);
        
        List<AnalysisResult> batch0 = returnedBatches.get(0);
        assertEquals(2, batch0.size());
        assertException("ConfigurationException: ANAYLSIS: Component class ", batch0.get(0));
        assertException("ConfigurationException: ANAYLSIS: Component class ", batch0.get(1));
        
        assertEquals(0, returnedBatches.get(1).size());
        
        assertEquals(0, returnedBatches.get(2).size());
        
        List<AnalysisResult> batch3 = returnedBatches.get(3);
        assertEquals(2, batch3.size());
        assertException("ConfigurationException: ANAYLSIS: Component class ", batch3.get(0));
        assertException("ConfigurationException: ANAYLSIS: Component class ", batch3.get(1));
	}

	@Test
	public void shouldProducePeriodicExceptionAnaylisisResultWithFilterConfigurationException() throws Exception {
		Monitors.getCache().reset();		
        Properties.initCache(null);
        Cache<Properties> propertiesCache = Properties.getCache();
        Properties properties = new Properties();
        properties.setProperty("monitor.mon1.filter.expr", "does not work");
		properties.setProperty("monitor.mon1.analysis.type", "true");
		properties.setProperty("monitor.mon1.analysis.error.upperbound", "20");
		properties.setProperty("monitor.mon1.analysis.error.lowerbound", "10");
        propertiesCache.set(properties);
	        
        addInput(0,    Metric(0, 0, "HOST=host1"));
        addInput(0,    Metric(20, 0, "HOST=host2345"));
        addInput(0,    Metric(40, 0, "HOST=host3454"));
        
        addInput(1,    Metric(60, 0, "HOST=host243"));
        addInput(1,    Metric(80, 0));
        
        addInput(2,    Metric(82, 0, "HOST=host243"));
        addInput(2,    Metric(85, 0));
        
        addInput(3,    Metric(100, 0, "HOST=host5426"));
        addInput(3,    Metric(120, 0, "HOST=ho"));
        addInput(3,    Metric(140, 0));
        Stream<Metric> metricsStream = createStream(Metric.class);
        
		Stream<AnalysisResult> results = Monitors.analyze(metricsStream, null);
        
        Batches<AnalysisResult> returnedBatches = collect(results);
        
        List<AnalysisResult> batch0 = returnedBatches.get(0);
        assertEquals(1, batch0.size());
        assertException("ConfigurationException: Error when parsing filter expression", batch0.get(0));
        
        List<AnalysisResult> batch1 = returnedBatches.get(1);
        assertEquals(1, batch1.size());
        assertException("ConfigurationException: Error when parsing filter expression", batch1.get(0));
        
        List<AnalysisResult> batch2 = returnedBatches.get(2);
        assertEquals(0, batch2.size());
        
        List<AnalysisResult> batch3 = returnedBatches.get(3);
        assertEquals(1, batch3.size());
        assertException("ConfigurationException: Error when parsing filter expression", batch3.get(0));
	}
	
	private void assertException(String exceptionStartsWith, AnalysisResult analysisResult) {
        assertEquals(Status.EXCEPTION, analysisResult.getStatus());
        
        assertTrue(analysisResult.getStatusReason().startsWith(exceptionStartsWith));
	}

}
