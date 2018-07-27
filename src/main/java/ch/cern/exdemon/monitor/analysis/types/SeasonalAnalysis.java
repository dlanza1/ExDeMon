package ch.cern.exdemon.monitor.analysis.types;

import java.time.Instant;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.metrics.predictor.LearningRatioValuePredictor;
import ch.cern.exdemon.metrics.predictor.Prediction;
import ch.cern.exdemon.monitor.analysis.NumericAnalysis;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.properties.Properties;
import ch.cern.spark.status.HasStatus;
import ch.cern.spark.status.StatusValue;

@RegisterComponentType("seasonal")
public class SeasonalAnalysis extends NumericAnalysis implements HasStatus{

    private static final long serialVersionUID = 6395895250358427351L;

    public static final String SEASON_PARAM = "season";
    public static final String SEASON_DEFAULT = LearningRatioValuePredictor.Period.HOUR.toString();
    private String season;

    public static final String LEARNING_RATIO_PARAM = "learning.ratio";
    public static final float LEARNING_RATIO_DEFAULT = 0.5f;
    private float learning_ratio;
    
    private LearningRatioValuePredictor predictor;
    
    public static String ERROR_RATIO_PARAM = "error.ratio";
    public static Float ERROR_RATIO_DEFAULT = 4f;
    private Float error_ratio;
    
    public static String WARNING_RATIO_PARAM = "warn.ratio";
    public static Float WARNING_RATIO_DEFAULT = 2f;
    private Float warning_ratio;

    public ConfigurationResult config(Properties properties) {
        ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
        
        learning_ratio = properties.getFloat(LEARNING_RATIO_PARAM, LEARNING_RATIO_DEFAULT);
        season = properties.getProperty(SEASON_PARAM, SEASON_DEFAULT).toUpperCase();    
        
        predictor = new LearningRatioValuePredictor(learning_ratio, LearningRatioValuePredictor.Period.valueOf(season));
        
        error_ratio = properties.getFloat(ERROR_RATIO_PARAM, ERROR_RATIO_DEFAULT);
        warning_ratio = properties.getFloat(WARNING_RATIO_PARAM, WARNING_RATIO_DEFAULT);
        
        return confResult.merge(null, properties.warningsIfNotAllPropertiesUsed());
    }
    
    @Override
    public void load(StatusValue store) {
        if(store == null){
            predictor = new LearningRatioValuePredictor(learning_ratio, LearningRatioValuePredictor.Period.valueOf(season));
        }else{
            predictor = ((LearningRatioValuePredictor.Status_) store).predictor;
            predictor.setLearningRatio(learning_ratio);
            predictor.setPeriod(LearningRatioValuePredictor.Period.valueOf(season));
        }
    }
    
    @Override
    public StatusValue save() {
        LearningRatioValuePredictor.Status_ store = new LearningRatioValuePredictor.Status_();
        
        store.predictor = predictor;
        
        return store;
    }

    @Override
    public AnalysisResult process(Instant timestamp, double value) {
        AnalysisResult result = new AnalysisResult();
        try{
            Prediction prediction = predictor.getPredictionForTime(timestamp);
            result.addAnalysisParam("prediction", prediction.getValue());
            
            processErrorUpperbound(result, value, prediction); 
            processWarningUpperbound(result, value, prediction);
            processErrorLowerbound(result, value, prediction);
            processWarningLowerbound(result, value, prediction);
            
            if(!result.hasStatus())
                result.setStatus(AnalysisResult.Status.OK, "Metric between thresholds");
        
        }catch(Exception e){
            result.setStatus(AnalysisResult.Status.EXCEPTION, e.getClass().getSimpleName() + ": " + e.getMessage());
        }
        
        predictor.addValue(timestamp, (float) value);
        
        return result;
    }
    
    private void processErrorLowerbound(AnalysisResult result, double value, Prediction prediction) {
        float threshold = prediction.getValue() - prediction.getStandardDeviation() * error_ratio;
        result.addAnalysisParam("error_lowerbound", threshold);
        
        if(!result.hasStatus() && value < threshold){
            result.setStatus(AnalysisResult.Status.ERROR, 
                    "Value (" + value + ") is less than " + threshold
                            + " = average (" + prediction.getValue()+ ")"
                            + " - standardDeviation (" + prediction.getStandardDeviation() + ")"
                            + " * error.ratio (" + error_ratio + ")");
        }
    }

    private void processWarningLowerbound(AnalysisResult result, double value, Prediction prediction) {
        float threshold = prediction.getValue() - prediction.getStandardDeviation() * warning_ratio;
        result.addAnalysisParam("warning_lowerbound", threshold);
        
        if(!result.hasStatus() && value < threshold){
            result.setStatus(AnalysisResult.Status.WARNING, 
                    "Value (" + value + ") is less than " + threshold
                            + " = average (" + prediction.getValue()+ ")"
                            + " - standardDeviation (" + prediction.getStandardDeviation() + ")"
                            + " * warn.ratio (" + error_ratio + ")");
        }
    }

    private void processWarningUpperbound(AnalysisResult result, double value, Prediction prediction) {
        float threshold = prediction.getValue() + prediction.getStandardDeviation() * warning_ratio;
        result.addAnalysisParam("warning_upperbound", threshold);
        
        if(!result.hasStatus() && value > threshold){
            result.setStatus(AnalysisResult.Status.WARNING, 
                    "Value (" + value + ") is higher than " + threshold
                            + " = average (" + prediction.getValue()+ ")"
                            + " + standardDeviation (" + prediction.getStandardDeviation() + ")"
                            + " * warn.ratio (" + error_ratio + ")");
        }
    }

    private void processErrorUpperbound(AnalysisResult result, double value, Prediction prediction) {
        float threshold = prediction.getValue() + prediction.getStandardDeviation() * error_ratio;
        result.addAnalysisParam("error_upperbound", threshold);
        
        if(!result.hasStatus() && value > threshold){
            result.setStatus(AnalysisResult.Status.ERROR, 
                    "Value (" + value + ") is higher than " + threshold
                            + " = average (" + prediction.getValue()+ ")"
                            + " + standardDeviation (" + prediction.getStandardDeviation() + ")"
                            + " * error.ratio (" + error_ratio + ")");
        }
    }

}
