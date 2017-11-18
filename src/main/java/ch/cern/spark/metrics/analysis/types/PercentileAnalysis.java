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
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.store.Store;
import ch.cern.spark.metrics.value.FloatValue;

@RegisterComponent("percentile")
public class PercentileAnalysis extends NumericAnalysis implements HasStore{
    
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

    public static String ERROR_PERCENTILE_PARAM = "error.percentile";
    public static float ERROR_PERCENTILE_DEFAULT = 99;
    private float error_percentile;
    
    public static String WARN_PERCENTILE_PARAM = "warn.percentile";
    public static float WARN_PERCENTILE_DEFAULT = 98;
    private float warn_percentile;
    
    public static String ERROR_RATIO_PARAM = "error.ratio";
    public static float ERROR_RATIO_DEFAULT = 0.3f;
    private float error_ratio;
    
    public static String WARN_RATIO_PARAM = "warn.ratio";
    public static float WARN_RATIO_DEFAULT = 0.2f;
    private float warn_ratio;

    @Override
    public void config(Properties properties) throws ConfigurationException {
        super.config(properties);
        
        error_upperbound = properties.getBoolean(ERROR_UPPERBOUND_PARAM);
        warning_upperbound = properties.getBoolean(WARNING_UPPERBOUND_PARAM);
        warning_lowerbound = properties.getBoolean(WARNING_LOWERBOUND_PARAM);
        error_lowerbound = properties.getBoolean(ERROR_LOWERBOUND_PARAM);
        
        error_percentile = properties.getFloat(ERROR_PERCENTILE_PARAM, ERROR_PERCENTILE_DEFAULT);
        if(error_percentile > 100 || error_percentile <=50)
            throw new ConfigurationException(ERROR_PERCENTILE_PARAM + " must be between 50 and 100");
        warn_percentile = properties.getFloat(WARN_PERCENTILE_PARAM, WARN_PERCENTILE_DEFAULT);
        if(warn_percentile > 100 || warn_percentile <=50)
            throw new ConfigurationException(WARN_PERCENTILE_PARAM + " must be between 50 and 100");
        
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
        
        if(history.size() < 5)
            return AnalysisResult.buildWithStatus(Status.EXCEPTION, "Not enought historic data (min 5 points)");
        
        AnalysisResult result = new AnalysisResult();

        double median = stats.getPercentile(50);
        
        processErrorUpperbound(result, value, stats, median); 
        processWarningUpperbound(result, value, stats, median);
        processErrorLowerbound(result, value, stats, median);
        processWarningLowerbound(result, value,stats, median);
        
        if(!result.hasStatus())
            result.setStatus(AnalysisResult.Status.OK, "Metric between thresholds");
        
        return result;
    }
    
    private void processErrorLowerbound(AnalysisResult result, double value, DescriptiveStatistics stats, double median) {
        if(!error_lowerbound)
            return;
        
        double percentile = stats.getPercentile(100 - error_percentile);
        double diff = Math.abs(median - percentile);
        double threshold = percentile - diff * error_ratio;
        
        result.addMonitorParam("error_lowerbound", threshold);
        
        if(result.hasStatus())
            return;
        
        if(value < threshold){
            result.setStatus(AnalysisResult.Status.ERROR, 
                    "Value (" + value + ") is less than "
                            + "percentil " + (100 - error_percentile) + " (" + percentile + ")"
                            + " - difference with median (" + diff + ")"
                            + " * error.ratio (" + error_ratio + ") " + " (=" + threshold + ")");
        }
    }

    private void processWarningLowerbound(AnalysisResult result, double value, DescriptiveStatistics stats, double median) {
        if(!warning_lowerbound)
            return;
        
        double percentile = stats.getPercentile(100 - warn_percentile);
        double diff = Math.abs(median - percentile);
        double threshold = percentile - diff * warn_ratio;

        result.addMonitorParam("warning_lowerbound", threshold);
        
        if(result.hasStatus())
            return;
        
        if(value < threshold){
            result.setStatus(AnalysisResult.Status.WARNING, 
                    "Value (" + value + ") is less than "
                            + "percentil " + (100 - warn_percentile) + " (" + percentile + ")"
                            + " - difference with median (" + diff + ")"
                            + " * warn.ratio (" + warn_ratio + ") " + " (=" + threshold + ")");
        }
    }

    private void processWarningUpperbound(AnalysisResult result, double value, DescriptiveStatistics stats, double median) {
        if(!warning_upperbound)
            return;
        
        double percentile = stats.getPercentile(warn_percentile);
        double diff = Math.abs(median - percentile);
        double threshold = percentile + diff * warn_ratio;
        
        result.addMonitorParam("warning_upperbound", threshold);
        
        if(result.hasStatus())
            return;
        
        if(value > threshold){
            result.setStatus(AnalysisResult.Status.WARNING, 
                    "Value (" + value + ") is more than "
                            + "percentil " + (warn_percentile) + " (" + percentile + ")"
                            + " + difference with median (" + diff + ")"
                            + " * warn.ratio (" + warn_ratio + ") " + " (=" + threshold + ")");
        }
    }

    private void processErrorUpperbound(AnalysisResult result, double value, DescriptiveStatistics stats, double median) {
        if(!error_upperbound)
            return;
        
        double percentile = stats.getPercentile(error_percentile);
        double diff = Math.abs(median - percentile);
        double threshold = percentile + diff * error_ratio;
        
        result.addMonitorParam("error_upperbound", threshold);
        
        if(result.hasStatus())
            return;
        
        if(value > threshold){
            result.setStatus(AnalysisResult.Status.ERROR, 
                    "Value (" + value + ") is more than "
                            + "percentil " + (error_percentile) + " (" + percentile + ")"
                            + " + difference with median (" + diff + ")"
                            + " * error.ratio (" + error_ratio + ") " + " (=" + threshold + ")");
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

    public ValueHistory getHistory() {
        return history;
    }

    public float getError_percentile() {
        return error_percentile;
    }

    public float getWarn_percentile() {
        return warn_percentile;
    }

    public float getError_ratio() {
        return error_ratio;
    }

    public float getWarn_ratio() {
        return warn_ratio;
    }

}
