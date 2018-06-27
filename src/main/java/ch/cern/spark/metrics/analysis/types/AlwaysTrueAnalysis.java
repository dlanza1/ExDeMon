package ch.cern.spark.metrics.analysis.types;

import java.time.Instant;

import ch.cern.components.RegisterComponentType;
import ch.cern.spark.metrics.analysis.BooleanAnalysis;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;

@RegisterComponentType("true")
public class AlwaysTrueAnalysis extends BooleanAnalysis {

	private static final long serialVersionUID = -4512609434171905694L;

	@Override
	public AnalysisResult process(Instant timestamp, boolean value) {
		if(value)
			return AnalysisResult.buildWithStatus(Status.OK, "Metric is true.");
		else
			return AnalysisResult.buildWithStatus(Status.ERROR, "Metric is false.");
	}

}
