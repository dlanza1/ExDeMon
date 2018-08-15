package ch.cern.exdemon.monitor.analysis.types.htm;

import static org.numenta.nupic.algorithms.Anomaly.KEY_ESTIMATION_SAMPLES;
import static org.numenta.nupic.algorithms.Anomaly.KEY_IS_WEIGHTED;
import static org.numenta.nupic.algorithms.Anomaly.KEY_LEARNING_PERIOD;
import static org.numenta.nupic.algorithms.Anomaly.KEY_USE_MOVING_AVG;
import static org.numenta.nupic.algorithms.Anomaly.KEY_WINDOW_SIZE;
import static org.numenta.nupic.algorithms.Anomaly.VALUE_NONE;

import java.util.Map;

import org.joda.time.DateTime;
import org.numenta.nupic.algorithms.AnomalyLikelihood;
import org.numenta.nupic.model.Persistable;
import org.numenta.nupic.network.ManualInput;
import org.numenta.nupic.util.NamedTuple;

import rx.functions.Func1;

public class AnomalyLikelihoodFunc implements Persistable, Func1<ManualInput, ManualInput> {

	private static final long serialVersionUID = -1696742457460907151L;
	private CustomAnomalyLikelihood anomalyLikelihood;
	private double errorThreshold;
	private double warningThreshold;
	
	
	public AnomalyLikelihoodFunc(double errorThreshold,double warningThreshold) {
		anomalyLikelihood = initAnomalyLikelihood(HTMParameters.getAnomalyLikelihoodParams());
		this.errorThreshold = errorThreshold;
		this.warningThreshold = warningThreshold;
	}
	
	@Override
	public ManualInput call(ManualInput I) {
		double anomalyScore = I.getAnomalyScore();
		Map<String, NamedTuple> inputs = I.getClassifierInput();
		
        double inputValue = (double) inputs.get("value").get("inputValue");
        DateTime timestamp = (DateTime) inputs.get("timestamp").get("inputValue");
        double al = anomalyLikelihood.anomalyProbability(inputValue, anomalyScore, timestamp);
        
        AnomaliesResults results = new AnomaliesResults(al, errorThreshold, warningThreshold);
        return I.customObject(results);
	}
	
	private static CustomAnomalyLikelihood initAnomalyLikelihood(Map<String, Object> anomalyParams) {
		
		boolean useMovingAvg = (boolean)anomalyParams.getOrDefault(KEY_USE_MOVING_AVG, false);
        int windowSize = (int)anomalyParams.getOrDefault(KEY_WINDOW_SIZE, -1);
        
        if(useMovingAvg && windowSize < 1) {
            throw new IllegalArgumentException("windowSize must be > 0, when using moving average.");
        }
		
        boolean isWeighted = (boolean)anomalyParams.getOrDefault(KEY_IS_WEIGHTED, false);
        int claLearningPeriod = (int)anomalyParams.getOrDefault(KEY_LEARNING_PERIOD, VALUE_NONE);
        int estimationSamples = (int)anomalyParams.getOrDefault(KEY_ESTIMATION_SAMPLES, VALUE_NONE);
        
		return new CustomAnomalyLikelihood(useMovingAvg, windowSize, isWeighted, claLearningPeriod, estimationSamples);
	}

}
