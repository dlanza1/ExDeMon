package ch.cern.spark.metrics.analysis;

import java.time.Instant;

import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;

public abstract class NumericAnalysis extends Analysis {

    private static final long serialVersionUID = -1822474093334300773L;

	@Override
	public AnalysisResult process(Metric metric) {
		if(!metric.getValue().getAsFloat().isPresent()) {
			AnalysisResult result = AnalysisResult.buildWithStatus(Status.EXCEPTION, "Current analysis requires metrics of float type."); 
			result.setAnalyzedMetric(metric);
			
			return result;
		}
		
		return process(metric.getInstant(), metric.getValue().getAsFloat().get());
	}

    public abstract AnalysisResult process(Instant timestamp, double value);

}
    
