package ch.cern.spark.metrics.preanalysis;

import java.time.Instant;

import ch.cern.spark.Component;

public abstract class PreAnalysis extends Component {

    private static final long serialVersionUID = -5502366780891060729L;
    
    public PreAnalysis() {
        super(Type.PRE_ANALYSIS);
    }
    
    public PreAnalysis(Class<? extends Component> subClass, String name) {
        super(Type.PRE_ANALYSIS, subClass, name);
    }

    public abstract double process(Instant metric_timestamp, double metric_value);
    
}
