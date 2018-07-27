package ch.cern.exdemon.monitor.analysis.types;

import java.time.Instant;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.monitor.analysis.NumericAnalysis;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.properties.Properties;

@RegisterComponentType("fixed-threshold")
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

    public ConfigurationResult config(Properties properties) {
        error_upperbound = properties.getFloat(ERROR_UPPERBOUND_PARAM);
        warning_upperbound = properties.getFloat(WARNING_UPPERBOUND_PARAM);
        warning_lowerbound = properties.getFloat(WARNING_LOWERBOUND_PARAM);
        error_lowerbound = properties.getFloat(ERROR_LOWERBOUND_PARAM);
        
        return properties.warningsIfNotAllPropertiesUsed();
    }
    
    @Override
    public AnalysisResult process(Instant timestamp, double value) {
        AnalysisResult result = new AnalysisResult();
        
        result.addAnalysisParam("error_upperbound", error_upperbound);
        result.addAnalysisParam("warning_upperbound", warning_upperbound);
        result.addAnalysisParam("warning_lowerbound", warning_lowerbound);
        result.addAnalysisParam("error_lowerbound", error_lowerbound);

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
