package ch.cern.exdemon.monitor.analysis.results.sink;

import ch.cern.exdemon.components.Component;
import ch.cern.exdemon.components.ComponentType;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.metrics.Sink;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;

@ComponentType(Type.ANALYSIS_RESULTS_SINK)
public abstract class AnalysisResultsSink extends Component implements Sink<AnalysisResult>{
    
    private static final long serialVersionUID = -2336360271932362626L;

}
