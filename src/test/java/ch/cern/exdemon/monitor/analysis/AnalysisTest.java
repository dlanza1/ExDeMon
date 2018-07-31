package ch.cern.exdemon.monitor.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.time.Instant;

import org.junit.Test;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.value.BooleanValue;
import ch.cern.exdemon.metrics.value.ExceptionValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;

public class AnalysisTest {
	
	private Analysis analysis = new Analysis() {
										private static final long serialVersionUID = 2630885101669119453L;
								
										@Override
										protected AnalysisResult process(Metric metric) {
											return AnalysisResult.buildWithStatus(Status.OK, "");
										}
									};
	
	@Test
	public void shouldGenerateExceptionAnalysisResultFromMetricInException() {		
		Metric metric = new Metric(Instant.now(), new ExceptionValue(""), null);
		AnalysisResult result = analysis.apply(metric);
		
		assertEquals(Status.EXCEPTION, result.getStatus());
		assertSame(metric, result.getAnalyzed_metric());
	}
	
	@Test
	public void shouldProcessMetricsWhichAreNotException() {		
		assertEquals(Status.OK, analysis.apply(new Metric(Instant.now(), new StringValue(""), null)).getStatus());
		assertEquals(Status.OK, analysis.apply(new Metric(Instant.now(), new FloatValue(0f), null)).getStatus());
		assertEquals(Status.OK, analysis.apply(new Metric(Instant.now(), new BooleanValue(true), null)).getStatus());
	}

}
