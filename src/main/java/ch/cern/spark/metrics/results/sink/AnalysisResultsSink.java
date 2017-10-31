package ch.cern.spark.metrics.results.sink;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.spark.metrics.Sink;
import ch.cern.spark.metrics.results.AnalysisResult;

@ComponentType(Type.ANALYSIS_RESULTS_SINK)
public abstract class AnalysisResultsSink extends Component implements Sink<AnalysisResult>{
    
    private static final long serialVersionUID = -2336360271932362626L;

}
