package ch.cern.spark.metrics.analysis.types;

import java.time.Instant;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.analysis.NumericAnalysis;
import ch.cern.spark.metrics.predictor.LearningRatioValuePredictor;
import ch.cern.spark.metrics.predictor.Prediction;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.store.Store;

@RegisterComponent("seasonal")
public class SeasonalAnalysis extends NumericAnalysis implements HasStore{

    private static final long serialVersionUID = 6395895250358427351L;

    public static String SEASON_PARAM = "season";
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

    public void config(Properties properties) throws ConfigurationException {
        super.config(properties);
        
        learning_ratio = properties.getFloat(LEARNING_RATIO_PARAM, LEARNING_RATIO_DEFAULT);
        season = properties.getProperty(SEASON_PARAM).toUpperCase();    
        
        predictor = new LearningRatioValuePredictor(learning_ratio, LearningRatioValuePredictor.Period.valueOf(season));
        
        error_ratio = properties.getFloat(ERROR_RATIO_PARAM, ERROR_RATIO_DEFAULT);
        warning_ratio = properties.getFloat(WARNING_RATIO_PARAM, WARNING_RATIO_DEFAULT);
    }
    
    @Override
    public void load(Store store) {
        if(store == null){
            predictor = new LearningRatioValuePredictor(learning_ratio, LearningRatioValuePredictor.Period.valueOf(season));
        }else{
            predictor = ((LearningRatioValuePredictor.Store_) store).predictor;
            predictor.setLearningRatio(learning_ratio);
            predictor.setPeriod(LearningRatioValuePredictor.Period.valueOf(season));
        }
    }
    
    @Override
    public Store save() {
        LearningRatioValuePredictor.Store_ store = new LearningRatioValuePredictor.Store_();
        
        store.predictor = predictor;
        
        return store;
    }

    @Override
    public AnalysisResult process(Instant timestamp, double value) {
        AnalysisResult result = new AnalysisResult();
        try{
            Prediction prediction = predictor.getPredictionForTime(timestamp);
            result.addMonitorParam("prediction", prediction.getValue());
            
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
        result.addMonitorParam("error_lowerbound", threshold);
        
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
        result.addMonitorParam("warning_lowerbound", threshold);
        
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
        result.addMonitorParam("warning_upperbound", threshold);
        
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
        result.addMonitorParam("error_upperbound", threshold);
        
        if(!result.hasStatus() && value > threshold){
            result.setStatus(AnalysisResult.Status.ERROR, 
                    "Value (" + value + ") is higher than " + threshold
                            + " = average (" + prediction.getValue()+ ")"
                            + " + standardDeviation (" + prediction.getStandardDeviation() + ")"
                            + " * error.ratio (" + error_ratio + ")");
        }
    }

}
