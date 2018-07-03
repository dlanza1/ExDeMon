package ch.cern.exdemon.monitor.analysis.types;

import java.time.Instant;

import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.monitor.analysis.BooleanAnalysis;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;

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
