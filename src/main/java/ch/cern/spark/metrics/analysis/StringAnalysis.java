package ch.cern.spark.metrics.analysis;

import java.time.Instant;

import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;

public abstract class StringAnalysis extends Analysis {

    private static final long serialVersionUID = -1822474093334300773L;

	@Override
	public AnalysisResult process(Metric metric) {
		if(!metric.getValue().getAsString().isPresent()) {
			AnalysisResult result = AnalysisResult.buildWithStatus(Status.EXCEPTION, "Current analysis requires metrics of string type."); 
			result.setAnalyzedMetric(metric);
			
	        return result;
		}
		
		return process(metric.getTimestamp(), metric.getValue().getAsString().get());
	}

    public abstract AnalysisResult process(Instant timestamp, String value);

}
    
