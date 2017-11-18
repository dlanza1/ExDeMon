package ch.cern.spark.metrics.analysis;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.junit.Test;

import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.StringValue;

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
		Metric metric = new Metric(null, new ExceptionValue(""), null);
		AnalysisResult result = analysis.apply(metric);
		
		assertEquals(Status.EXCEPTION, result.getStatus());
		assertSame(metric, result.getAnalyzedMetric());
	}
	
	@Test
	public void shouldProcessMetricsWhichAreNotException() {		
		assertEquals(Status.OK, analysis.apply(new Metric(null, new StringValue(""), null)).getStatus());
		assertEquals(Status.OK, analysis.apply(new Metric(null, new FloatValue(0f), null)).getStatus());
		assertEquals(Status.OK, analysis.apply(new Metric(null, new BooleanValue(true), null)).getStatus());
	}

}
