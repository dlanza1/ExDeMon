package ch.cern.exdemon.monitor.analysis.types;

import static ch.cern.exdemon.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;;

public class AlwaysTrueAnalysisTest {
	
	@Test
	public void analysis() {
		
		AlwaysTrueAnalysis analysis = new AlwaysTrueAnalysis();
		
		Metric metric = Metric(0, true);
		assertEquals(Status.OK, analysis.apply(metric).getStatus());
		
		metric = Metric(0, false);
		assertEquals(Status.ERROR, analysis.apply(metric).getStatus());
	}

}
