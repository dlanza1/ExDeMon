package ch.cern.spark.metrics.analysis;

import java.time.Instant;
import java.util.function.Function;

import ch.cern.Component;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;

public abstract class Analysis extends Component implements Function<Metric, AnalysisResult> {

    private static final long serialVersionUID = -1822474093334300773L;
    
    public Analysis() {
        super(Type.ANAYLSIS);
    }
    
    public Analysis(Class<? extends Analysis> subClass, String name) {
        super(Type.ANAYLSIS, subClass, name);
    }
	
	public final AnalysisResult apply(Metric metric) {
		return process(metric.getInstant(), metric.getValue());
	}
	
    public abstract AnalysisResult process(Instant timestamp, double value);

}
    
