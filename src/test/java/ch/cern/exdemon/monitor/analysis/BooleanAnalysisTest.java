package ch.cern.exdemon.monitor.analysis;

import static org.junit.Assert.assertEquals;

import java.time.Instant;

import org.junit.Test;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.value.BooleanValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;

public class BooleanAnalysisTest {
	
	private BooleanAnalysis analysis = new BooleanAnalysis() {
										private static final long serialVersionUID = 2630885101669119453L;
								
										@Override
										public AnalysisResult process(Instant timestamp, boolean value) {
											return AnalysisResult.buildWithStatus(Status.OK, "");
										}
									};
	
	@Test
	public void shouldAnalyzeOnlyStringMetric() {		
		assertEquals(Status.EXCEPTION, analysis.apply(new Metric(Instant.now(), new StringValue(""), null)).getStatus());
		assertEquals(Status.EXCEPTION, analysis.apply(new Metric(Instant.now(), new FloatValue(0f), null)).getStatus());
		assertEquals(Status.OK, analysis.apply(new Metric(Instant.now(), new BooleanValue(true), null)).getStatus());
	}

}
