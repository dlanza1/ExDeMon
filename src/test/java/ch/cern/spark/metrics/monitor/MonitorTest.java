package ch.cern.spark.metrics.monitor;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.HashMap;

import org.junit.Test;

import ch.cern.spark.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.store.MetricStore;

public class MonitorTest {
	
	@Test
	public void monitorWithOutPreAnalysis() throws Exception {
		
		Properties properties = new Properties();
		properties.setProperty("analysis.type", "fixed-threshold");
		properties.setProperty("analysis.error.upperbound", "20");
		properties.setProperty("analysis.error.lowerbound", "10");
		Monitor monitor = new Monitor(null).config(properties);
		
		MetricStore store = new MetricStore();
		
		AnalysisResult result = monitor.process(store, new Metric(Instant.now(), 0f, new HashMap<>()));
		assertEquals(AnalysisResult.Status.ERROR, result.getStatus());
		
		result = monitor.process(store, new Metric(Instant.now(), 15f, new HashMap<>()));
		assertEquals(AnalysisResult.Status.OK, result.getStatus());
	}
	
	@Test
	public void monitorWithPreAnalysis() throws Exception {
		
		Properties properties = new Properties();
		properties.setProperty("pre-analysis.type", "difference");
		properties.setProperty("analysis.type", "fixed-threshold");
		properties.setProperty("analysis.error.upperbound", "10");
		
		Monitor monitor = new Monitor(null).config(properties);
		
		MetricStore store = new MetricStore();
		
		AnalysisResult result = monitor.process(store, new Metric(Instant.now(), 0f, new HashMap<>()));
		assertEquals(0f, result.getMonitorParams().get("preAnalyzedValue"));
		assertEquals(AnalysisResult.Status.OK, result.getStatus());
		
		result = monitor.process(store, new Metric(Instant.now(), 5f, new HashMap<>()));
		assertEquals(5f, result.getMonitorParams().get("preAnalyzedValue"));
		assertEquals(AnalysisResult.Status.OK, result.getStatus());
		
		result = monitor.process(store, new Metric(Instant.now(), 0f, new HashMap<>()));
		assertEquals(-5f, result.getMonitorParams().get("preAnalyzedValue"));
		assertEquals(AnalysisResult.Status.OK, result.getStatus());
		
		result = monitor.process(store, new Metric(Instant.now(), 20f, new HashMap<>()));
		assertEquals(20f, result.getMonitorParams().get("preAnalyzedValue"));
		assertEquals(AnalysisResult.Status.ERROR, result.getStatus());
	}

}
