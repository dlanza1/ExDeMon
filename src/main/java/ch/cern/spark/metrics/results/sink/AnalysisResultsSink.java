package ch.cern.spark.metrics.results.sink;

import ch.cern.spark.Component;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.results.AnalysisResult;

public abstract class AnalysisResultsSink extends Component{
    
    private static final long serialVersionUID = -2336360271932362626L;

    public AnalysisResultsSink() {
        super(Type.ANALYSIS_RESULTS_SINK);
    }
    
    public AnalysisResultsSink(Class<? extends Component> subClass, String name) {
        super(Type.ANALYSIS_RESULTS_SINK, subClass, name);
    }
    
    public abstract void sink(Stream<AnalysisResult> results);
    
}
