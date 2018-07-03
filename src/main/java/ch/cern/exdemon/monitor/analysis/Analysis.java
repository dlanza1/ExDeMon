package ch.cern.exdemon.monitor.analysis;

import java.util.function.Function;

import ch.cern.exdemon.components.Component;
import ch.cern.exdemon.components.ComponentType;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;

@ComponentType(Type.ANAYLSIS)
public abstract class Analysis extends Component implements Function<Metric, AnalysisResult> {

    private static final long serialVersionUID = -1822474093334300773L;
    
	public final AnalysisResult apply(Metric metric) {
		if(metric.getValue().getAsException().isPresent()) {
			AnalysisResult result = AnalysisResult.buildWithStatus(Status.EXCEPTION, "Metric of type excpetion."); 
			result.setAnalyzedMetric(metric);
			
	        return result;
		}
		
		return process(metric);
	}
	
    protected abstract AnalysisResult process(Metric metric);
	
}
    
