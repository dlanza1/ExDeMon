package ch.cern.spark.metrics.results.sink;

import ch.cern.Component;
import ch.cern.spark.metrics.Sink;
import ch.cern.spark.metrics.results.AnalysisResult;

public abstract class AnalysisResultsSink extends Component implements Sink<AnalysisResult>{
    
    private static final long serialVersionUID = -2336360271932362626L;

    public AnalysisResultsSink() {
        super(Type.ANALYSIS_RESULTS_SINK);
    }
    
    public AnalysisResultsSink(Class<? extends Component> subClass, String name) {
        super(Type.ANALYSIS_RESULTS_SINK, subClass, name);
    }

}
