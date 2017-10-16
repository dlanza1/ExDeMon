package ch.cern.spark.metrics.preanalysis;

import java.time.Instant;
import java.util.function.Function;

import ch.cern.Component;
import ch.cern.spark.metrics.Metric;

public abstract class PreAnalysis extends Component implements Function<Metric, Metric>{

    private static final long serialVersionUID = -5502366780891060729L;
    
    public PreAnalysis() {
        super(Type.PRE_ANALYSIS);
    }
    
    public PreAnalysis(Class<? extends Component> subClass, String name) {
        super(Type.PRE_ANALYSIS, subClass, name);
    }

	public final Metric apply(Metric metric) {
		double newValue = process(metric.getInstant(), metric.getValue());
		
		return new Metric(metric.getInstant(), (float) newValue, metric.getIDs());
	}
	
    public abstract double process(Instant metric_timestamp, double metric_value);
    
}
