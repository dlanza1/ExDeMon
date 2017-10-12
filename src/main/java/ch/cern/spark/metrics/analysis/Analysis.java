package ch.cern.spark.metrics.analysis;

import java.time.Instant;

import ch.cern.spark.Component;
import ch.cern.spark.metrics.results.AnalysisResult;

public abstract class Analysis extends Component {

    private static final long serialVersionUID = -1822474093334300773L;
    
    public Analysis() {
        super(Type.ANAYLSIS);
    }
    
    public Analysis(Class<? extends Analysis> subClass, String name) {
        super(Type.ANAYLSIS, subClass, name);
    }

    public abstract AnalysisResult process(Instant timestamp, double value);

}
    