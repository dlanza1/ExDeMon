package ch.cern.exdemon.monitor.analysis.types;

import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.analysis.Analysis;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;

@RegisterComponentType("none")
public class NoneAnalysis extends Analysis {

	private static final long serialVersionUID = 335288998662554717L;

	@Override
	protected AnalysisResult process(Metric metric) {
		return AnalysisResult.buildWithStatus(Status.OK, "No analysis");
	}

}
