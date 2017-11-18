package ch.cern.spark.metrics.analysis.types;

import java.time.Duration;
import java.time.Instant;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.ValueHistory;
import ch.cern.spark.metrics.analysis.NumericAnalysis;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.store.Store;
import ch.cern.spark.metrics.value.FloatValue;

@RegisterComponent("recent")
public class RecentActivityAnalysis extends NumericAnalysis implements HasStore{
    
    private static final long serialVersionUID = 5419076430764447352L;
    
    public static final String PERIOD_PARAM = "period";
    public static final Duration PERIOD_DEFAULT = Duration.ofMinutes(5);
    private Duration period;
    
    public static String ERROR_UPPERBOUND_PARAM = "error.upperbound";
    private boolean error_upperbound = false;
    
    public static String WARNING_UPPERBOUND_PARAM = "warn.upperbound";
    private boolean warning_upperbound = false;
    
    public static String WARNING_LOWERBOUND_PARAM = "warn.lowerbound";
    private boolean warning_lowerbound = false;
    
    public static String ERROR_LOWERBOUND_PARAM = "error.lowerbound";
    private boolean error_lowerbound = false;
    
    private ValueHistory history;

    public static String ERROR_RATIO_PARAM = "error.ratio";
    public static float ERROR_RATIO_DEFAULT = 1.8f;
    private float error_ratio;
    
    public static String WARN_RATIO_PARAM = "warn.ratio";
    public static float WARN_RATIO_DEFAULT = 1.5f;
    private float warn_ratio;

    @Override
    public void config(Properties properties) throws ConfigurationException {
        super.config(properties);
        
        error_upperbound = properties.getBoolean(ERROR_UPPERBOUND_PARAM);
        warning_upperbound = properties.getBoolean(WARNING_UPPERBOUND_PARAM);
        warning_lowerbound = properties.getBoolean(WARNING_LOWERBOUND_PARAM);
        error_lowerbound = properties.getBoolean(ERROR_LOWERBOUND_PARAM);
        
        error_ratio = properties.getFloat(ERROR_RATIO_PARAM, ERROR_RATIO_DEFAULT);
        warn_ratio = properties.getFloat(WARN_RATIO_PARAM, WARN_RATIO_DEFAULT);
        
        period = properties.getPeriod(PERIOD_PARAM, PERIOD_DEFAULT);
        history = new ValueHistory(period);
    }
    
    @Override
    public void load(Store store) {
        if(store == null){
            history = new ValueHistory(period);
        }else{
            history = ((ValueHistory.Store_) store).history;
            history.setPeriod(period);
        }
    }
    
    @Override
    public Store save() {
        ValueHistory.Store_ store = new ValueHistory.Store_();
        
        store.history = history;
        
        return store;
    }

    @Override
    public AnalysisResult process(Instant timestamp, double value) {
        history.purge(timestamp);
        
        DescriptiveStatistics stats = history.getStatistics();

        history.add(timestamp, new FloatValue(value));
        
        AnalysisResult result = new AnalysisResult();
        
        double median = stats.getMean();
        double variance = stats.getStandardDeviation();
        
        processErrorUpperbound(result, value, median, variance); 
        processWarningUpperbound(result, value, median, variance);
        processErrorLowerbound(result, value, median, variance);
        processWarningLowerbound(result, value, median, variance);
        
        if(!result.hasStatus())
            result.setStatus(AnalysisResult.Status.OK, "Metric between thresholds");
        
        return result;
    }
    
    private void processErrorLowerbound(AnalysisResult result, double value, double average, double variance) {
        if(!error_lowerbound)
            return;
        
        double error_lowerbound_value = average - variance * error_ratio ;
        result.addMonitorParam("error_lowerbound", error_lowerbound_value);
        
        if(result.hasStatus())
            return;
        
        if(value < error_lowerbound_value){
            result.setStatus(AnalysisResult.Status.ERROR, 
                    "Value (" + value + ") is less than "
                            + "average value from history minus variance * " + ERROR_RATIO_PARAM
                            + " (" + average + " - (" + variance + " * " + error_ratio  + ") = " + error_lowerbound_value + ")");
        }
    }

    private void processWarningLowerbound(AnalysisResult result, double value, double average, double variance) {
        if(!warning_lowerbound)
            return;
        
        double warning_lowerbound_value = average - variance * warn_ratio;
        result.addMonitorParam("warning_lowerbound", warning_lowerbound_value);
        
        if(result.hasStatus())
            return;
        
        if(value < warning_lowerbound_value){
            result.setStatus(AnalysisResult.Status.WARNING, 
                    "Value (" + value + ") is less than "
                            + "average value from history minus std deviation " + WARN_RATIO_PARAM
                            + " (" + average + " - (" + variance + " * " + warn_ratio  + ") = " + warning_lowerbound_value + ")");
        }
    }

    private void processWarningUpperbound(AnalysisResult result, double value, double average, double variance) {
        if(!warning_upperbound)
            return;
        
        double warning_upperbound_value = average + variance * warn_ratio;
        result.addMonitorParam("warning_upperbound", warning_upperbound_value);
        
        if(result.hasStatus())
            return;
        
        if(value > warning_upperbound_value){
            result.setStatus(AnalysisResult.Status.WARNING, 
                    "Value (" + value + ") is higher than "
                            + "average value from history plus variance * " + WARN_RATIO_PARAM
                            + " (" + average + " + (" + variance + " * " + warn_ratio  + ") = " + warning_upperbound_value + ")");
        }
    }

    private void processErrorUpperbound(AnalysisResult result, double value, double average, double variance) {
        if(!error_upperbound)
            return;
        
        double error_upperbound_value = average + variance * error_ratio;
        result.addMonitorParam("error_upperbound", error_upperbound_value);
        
        if(result.hasStatus())
            return;
        
        if(value > error_upperbound_value){
            result.setStatus(AnalysisResult.Status.ERROR, 
                    "Value (" + value + ") is higher than "
                            + "average value from history plus variance * " + ERROR_RATIO_PARAM
                            + " (" + average + " + (" + variance + " * " + error_ratio  + ") = " + error_upperbound_value + ")");
        }
    }

    public Duration getPeriod() {
        return period;
    }

    public boolean isError_upperbound() {
        return error_upperbound;
    }

    public boolean isWarning_upperbound() {
        return warning_upperbound;
    }

    public boolean isWarning_lowerbound() {
        return warning_lowerbound;
    }

    public boolean isError_lowerbound() {
        return error_lowerbound;
    }

    public float getError_ratio() {
        return error_ratio;
    }

    public float getWarn_ratio() {
        return warn_ratio;
    }

}
