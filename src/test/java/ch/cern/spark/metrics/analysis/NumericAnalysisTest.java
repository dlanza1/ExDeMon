package ch.cern.spark.metrics.analysis;

import static org.junit.Assert.assertEquals;

import java.time.Instant;

import org.junit.Test;

import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.StringValue;

public class NumericAnalysisTest {
	
	private NumericAnalysis analysis = new NumericAnalysis() {
										private static final long serialVersionUID = 2630885101669119453L;
								
										@Override
										public AnalysisResult process(Instant timestamp, double value) {
											return AnalysisResult.buildWithStatus(Status.OK, "");
										}
									};
	
	@Test
	public void shouldAnalyzeOnlyStringMetric() {		
		assertEquals(Status.EXCEPTION, analysis.apply(new Metric(null, new StringValue(""), null)).getStatus());
		assertEquals(Status.OK, analysis.apply(new Metric(null, new FloatValue(0f), null)).getStatus());
		assertEquals(Status.EXCEPTION, analysis.apply(new Metric(null, new BooleanValue(true), null)).getStatus());
	}

}
