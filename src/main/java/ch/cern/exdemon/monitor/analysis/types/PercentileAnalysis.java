package ch.cern.exdemon.monitor.analysis.types;

import java.time.Duration;
import java.time.Instant;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.metrics.ValueHistory;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.monitor.analysis.NumericAnalysis;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.HasStatus;
import ch.cern.spark.status.StatusValue;
import lombok.Getter;
import lombok.ToString;

@ToString
@RegisterComponentType("percentile")
public class PercentileAnalysis extends NumericAnalysis implements HasStatus{
    
    private static final long serialVersionUID = 5419076430764447352L;
    
    public static final String PERIOD_PARAM = "period";
    public static final Duration PERIOD_DEFAULT = Duration.ofMinutes(5);
    @Getter
    private Duration period;
    
    public static String ERROR_UPPERBOUND_PARAM = "error.upperbound";
    @Getter
    private boolean error_upperbound = false;
    
    public static String WARNING_UPPERBOUND_PARAM = "warn.upperbound";
    @Getter
    private boolean warning_upperbound = false;
    
    public static String WARNING_LOWERBOUND_PARAM = "warn.lowerbound";
    @Getter
    private boolean warning_lowerbound = false;
    
    public static String ERROR_LOWERBOUND_PARAM = "error.lowerbound";
    @Getter
    private boolean error_lowerbound = false;
    
    private ValueHistory history;

    public static String ERROR_PERCENTILE_PARAM = "error.percentile";
    public static float ERROR_PERCENTILE_DEFAULT = 99;
    @Getter
    private float error_percentile;
    
    public static String WARN_PERCENTILE_PARAM = "warn.percentile";
    public static float WARN_PERCENTILE_DEFAULT = 98;
    @Getter
    private float warn_percentile;
    
    public static String ERROR_RATIO_PARAM = "error.ratio";
    public static float ERROR_RATIO_DEFAULT = 0.3f;
    @Getter
    private float error_ratio;
    
    public static String WARN_RATIO_PARAM = "warn.ratio";
    public static float WARN_RATIO_DEFAULT = 0.2f;
    @Getter
    private float warn_ratio;

    @Override
    public ConfigurationResult config(Properties properties) {
        ConfigurationResult configResult = ConfigurationResult.SUCCESSFUL();
        
        try {
            error_upperbound = properties.getBoolean(ERROR_UPPERBOUND_PARAM);
        } catch (ConfigurationException e) {
            configResult.withError(null, e);
        }
        try {
            warning_upperbound = properties.getBoolean(WARNING_UPPERBOUND_PARAM);
        } catch (ConfigurationException e) {
            configResult.withError(null, e);
        }
        try {
            warning_lowerbound = properties.getBoolean(WARNING_LOWERBOUND_PARAM);
        } catch (ConfigurationException e) {
            configResult.withError(null, e);
        }
        try {
            error_lowerbound = properties.getBoolean(ERROR_LOWERBOUND_PARAM);
        } catch (ConfigurationException e) {
            configResult.withError(null, e);
        }
        
        error_percentile = properties.getFloat(ERROR_PERCENTILE_PARAM, ERROR_PERCENTILE_DEFAULT);
        if(error_percentile > 100 || error_percentile <=50)
            configResult.withError(ERROR_PERCENTILE_PARAM, " must be between 50 and 100");
        warn_percentile = properties.getFloat(WARN_PERCENTILE_PARAM, WARN_PERCENTILE_DEFAULT);
        if(warn_percentile > 100 || warn_percentile <=50)
            configResult.withError(WARN_PERCENTILE_PARAM, " must be between 50 and 100");
        
        error_ratio = properties.getFloat(ERROR_RATIO_PARAM, ERROR_RATIO_DEFAULT);
        warn_ratio = properties.getFloat(WARN_RATIO_PARAM, WARN_RATIO_DEFAULT);
        
        try {
            period = properties.getPeriod(PERIOD_PARAM, PERIOD_DEFAULT);
        } catch (ConfigurationException e) {
            configResult.withError(null, e);
        }
        history = new ValueHistory();
        
        return configResult.merge(null, properties.warningsIfNotAllPropertiesUsed());
    }
    
    @Override
    public void load(StatusValue store) {
        if(store == null){
            history = new ValueHistory();
        }else{
            history = ((ValueHistory.Status) store).history;
        }
    }
    
    @Override
    public StatusValue save() {
        ValueHistory.Status store = new ValueHistory.Status();
        
        store.history = history;
        
        return store;
    }

    @Override
    public AnalysisResult process(Instant timestamp, double value) {
        if(period != null)
            history.purge(timestamp.minus(period));
        
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
        
        result.addAnalysisParam("error_lowerbound", threshold);
        
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

        result.addAnalysisParam("warning_lowerbound", threshold);
        
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
        
        result.addAnalysisParam("warning_upperbound", threshold);
        
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
        
        result.addAnalysisParam("error_upperbound", threshold);
        
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

}
