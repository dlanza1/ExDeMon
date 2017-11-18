package ch.cern.spark.metrics.analysis.types;

import java.time.Instant;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.analysis.NumericAnalysis;
import ch.cern.spark.metrics.results.AnalysisResult;

@RegisterComponent("fixed-threshold")
public class FixedThresholdAnalysis extends NumericAnalysis {
    
    private static final long serialVersionUID = -6079216045777381098L;
    
    public static String ERROR_UPPERBOUND_PARAM = "error.upperbound";
    private Float error_upperbound;
    
    public static String WARNING_UPPERBOUND_PARAM = "warn.upperbound";
    private Float warning_upperbound;
    
    public static String WARNING_LOWERBOUND_PARAM = "warn.lowerbound";
    private Float warning_lowerbound;
    
    public static String ERROR_LOWERBOUND_PARAM = "error.lowerbound";
    private Float error_lowerbound;

    public void config(Properties properties) throws ConfigurationException {
        error_upperbound = properties.getFloat(ERROR_UPPERBOUND_PARAM);
        warning_upperbound = properties.getFloat(WARNING_UPPERBOUND_PARAM);
        warning_lowerbound = properties.getFloat(WARNING_LOWERBOUND_PARAM);
        error_lowerbound = properties.getFloat(ERROR_LOWERBOUND_PARAM);
    }
    
    @Override
    public AnalysisResult process(Instant timestamp, double value) {
        AnalysisResult result = new AnalysisResult();
        
        result.addMonitorParam("error_upperbound", error_upperbound);
        result.addMonitorParam("warning_upperbound", warning_upperbound);
        result.addMonitorParam("warning_lowerbound", warning_lowerbound);
        result.addMonitorParam("error_lowerbound", error_lowerbound);

        if(error_upperbound != null && value >= error_upperbound){
            result.setStatus(AnalysisResult.Status.ERROR, 
                    "Value (" + value + ") is higher or equal than error.upperbound (" + error_upperbound + ")");
        }else if(warning_upperbound != null && value >= warning_upperbound){
            result.setStatus(AnalysisResult.Status.WARNING, 
                    "Value (" + value + ") is higher or equal than warn.upperbound (" + warning_upperbound + ")");
        }else if(error_lowerbound != null && value <= error_lowerbound){
            result.setStatus(AnalysisResult.Status.ERROR, 
                    "Value (" + value + ") is less or equal than error.lowerbound (" + error_lowerbound + ")");
        }else if(warning_lowerbound != null && value <= warning_lowerbound){
            result.setStatus(AnalysisResult.Status.WARNING, 
                    "Value (" + value + ") is less or equal than warn.lowerbound (" + warning_lowerbound + ")");
        }else{
            result.setStatus(AnalysisResult.Status.OK, "Metric between thresholds");
        }
        
        return result;
    }
    
    public Float getErrorUpperBound() {
        return error_upperbound;
    }
    
    public Float getErrorLowerBound() {
        return error_lowerbound;
    }
    
    public Float getWarningUpperBound() {
        return warning_upperbound;
    }
    
    public Float getWarningLowerBound() {
        return warning_lowerbound;
    }

}
