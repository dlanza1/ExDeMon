package ch.cern.spark.metrics.analysis.types;

import static ch.cern.spark.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;

import org.junit.Test;

import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult.Status;;

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
