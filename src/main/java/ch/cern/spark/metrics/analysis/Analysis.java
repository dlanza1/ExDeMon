package ch.cern.spark.metrics.analysis;

import java.time.Instant;
import java.util.function.Function;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;

@ComponentType(Type.ANAYLSIS)
public abstract class Analysis extends Component implements Function<Metric, AnalysisResult> {

    private static final long serialVersionUID = -1822474093334300773L;
    
	public final AnalysisResult apply(Metric metric) {
		return process(metric.getInstant(), metric.getValue());
	}
	
    public abstract AnalysisResult process(Instant timestamp, double value);

}
    
