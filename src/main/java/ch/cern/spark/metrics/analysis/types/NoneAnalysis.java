package ch.cern.spark.metrics.analysis.types;

import ch.cern.components.RegisterComponentType;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.analysis.Analysis;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;

@RegisterComponentType("none")
public class NoneAnalysis extends Analysis {

	private static final long serialVersionUID = 335288998662554717L;

	@Override
	protected AnalysisResult process(Metric metric) {
		return AnalysisResult.buildWithStatus(Status.OK, "No analysis");
	}

}
