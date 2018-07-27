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
import ch.cern.spark.status.storage.ClassNameAlias;
import lombok.Getter;
import lombok.ToString;

@ToString
@RegisterComponentType("recent")
public class RecentActivityAnalysis extends NumericAnalysis implements HasStatus {

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
    private double mean = Double.MIN_VALUE;
    private double variance = 0;
    
    public static String ERROR_RATIO_PARAM = "error.ratio";
    public static float ERROR_RATIO_DEFAULT = 1.8f;
    @Getter
    private float error_ratio;

    public static String WARN_RATIO_PARAM = "warn.ratio";
    public static float WARN_RATIO_DEFAULT = 1.5f;
    @Getter
    private float warn_ratio;
    
    public static String LEARNING_RATIO_PARAM = "learning.ratio";
    @Getter
    private Float learning_ratio;

    public static String LEARNING_UPPERBOUND_RATIO_PARAM = "learning.upperbound.ratio";
    private Float learning_upperbound_ratio;

    public static String LEARNING_LOWERBOUND_RATIO_PARAM = "learning.lowerbound.ratio";
    private Float learning_lowerbound_ratio;

    private long count = 0;

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

        learning_ratio = properties.getFloat(LEARNING_RATIO_PARAM);
        
        error_ratio = properties.getFloat(ERROR_RATIO_PARAM, ERROR_RATIO_DEFAULT);
        warn_ratio = properties.getFloat(WARN_RATIO_PARAM, WARN_RATIO_DEFAULT);

        learning_upperbound_ratio = properties.getFloat(LEARNING_UPPERBOUND_RATIO_PARAM);
        learning_lowerbound_ratio = properties.getFloat(LEARNING_LOWERBOUND_RATIO_PARAM);

        try {
            period = properties.getPeriod(PERIOD_PARAM, PERIOD_DEFAULT);
        } catch (ConfigurationException e) {
            configResult.withError(null, e);
        }
        history = new ValueHistory();

        return configResult.merge(null, properties.warningsIfNotAllPropertiesUsed());
    }

    @Override
    public void load(StatusValue status) {
        if (status != null && (status instanceof Status_)) {
            Status_ status_ = ((Status_) status);
            
            history = status_.history;
            count = status_.count;
            mean = status_.mean;
            variance = status_.variance;
        }
        
        if(history == null)
            history = new ValueHistory();
    }

    @Override
    public StatusValue save() {
        Status_ status = new Status_();

        status.history = history;
        status.count = count;
        status.mean = mean;
        status.variance = variance;

        return status;
    }

    @Override
    public AnalysisResult process(Instant timestamp, double value) {
        AnalysisResult result = new AnalysisResult();
        
        if(mean == Double.MIN_VALUE) {
            result.setStatus(Status.EXCEPTION, "History is empty.");
        }else {
            result.addAnalysisParam("mean", mean);
            result.addAnalysisParam("variance", variance);

            processErrorUpperbound(result, value);
            processWarningUpperbound(result, value);
            processErrorLowerbound(result, value);
            processWarningLowerbound(result, value);
        }

        if (!result.hasStatus())
            result.setStatus(AnalysisResult.Status.OK, "Metric between thresholds");
        
        learnValue(result, timestamp, value);

        return result;
    }

    private void learnValue(AnalysisResult result, Instant timestamp, double value) {
        if(isLearningRatioConfigured()) {
            if(mean == Double.MIN_VALUE) {
                mean = value;
                variance = 0;
                
                count++;
            }else{
                if(count < 10 || isBetweenLearningRatio(result, value)) {
                    double diff = Math.abs(value - mean);
                    
                    if(mean != value)
                        mean = mean * (1 - learning_ratio) + value * learning_ratio;
                    
                    if(variance == 0f)
                        variance = diff;
                    variance = variance * (1 - learning_ratio) + diff * learning_ratio;
                    
                    count++;
                }
            }
        }else {            
            if(history.size() < 10 || isBetweenLearningRatio(result, value)) {
                history.add(timestamp, new FloatValue(value));
            }
            
            if(history.size() > 0) {
                history.purge(timestamp.minus(period));
                
                DescriptiveStatistics stats = history.getStatistics();

                mean = stats.getMean();
                variance = stats.getStandardDeviation();
            }
            
            count = history.size();
        }
    }

    private boolean isLearningRatioConfigured() {
        return learning_ratio != null;
    }

    private boolean isBetweenLearningRatio(AnalysisResult resultvalue, double value) {
        Double learning_upperbound = null;
        if(learning_upperbound_ratio != null) {
            learning_upperbound = mean + variance * learning_upperbound_ratio;
            resultvalue.addAnalysisParam("learning_upperbound", learning_upperbound);
        }
        
        Double learning_lowerbound = null;
        if(learning_lowerbound_ratio != null) {
            learning_lowerbound = mean - variance * learning_lowerbound_ratio;
            resultvalue.addAnalysisParam("learning_lowerbound", learning_lowerbound);
        }
        
        return variance == 0 || 
                (learning_upperbound == null || learning_upperbound > value) 
                && 
                (learning_lowerbound == null || value > learning_lowerbound);
    }

    private void processErrorLowerbound(AnalysisResult result, double value) {
        if (!error_lowerbound)
            return;

        double error_lowerbound_value = mean - variance * error_ratio;
        result.addAnalysisParam("error_lowerbound", error_lowerbound_value);

        if (result.hasStatus())
            return;

        if (value < error_lowerbound_value) {
            result.setStatus(AnalysisResult.Status.ERROR,
                    "Value (" + value + ") is less than " + "average value from history minus variance * "
                            + ERROR_RATIO_PARAM + " (" + mean + " - (" + variance + " * " + error_ratio + ") = "
                            + error_lowerbound_value + ")");
        }
    }

    private void processWarningLowerbound(AnalysisResult result, double value) {
        if (!warning_lowerbound)
            return;

        double warning_lowerbound_value = mean - variance * warn_ratio;
        result.addAnalysisParam("warning_lowerbound", warning_lowerbound_value);

        if (result.hasStatus())
            return;

        if (value < warning_lowerbound_value) {
            result.setStatus(AnalysisResult.Status.WARNING,
                    "Value (" + value + ") is less than " + "average value from history minus std deviation "
                            + WARN_RATIO_PARAM + " (" + mean + " - (" + variance + " * " + warn_ratio + ") = "
                            + warning_lowerbound_value + ")");
        }
    }

    private void processWarningUpperbound(AnalysisResult result, double value) {
        if (!warning_upperbound)
            return;

        double warning_upperbound_value = mean + variance * warn_ratio;
        result.addAnalysisParam("warning_upperbound", warning_upperbound_value);

        if (result.hasStatus())
            return;

        if (value > warning_upperbound_value) {
            result.setStatus(AnalysisResult.Status.WARNING,
                    "Value (" + value + ") is higher than " + "average value from history plus variance * "
                            + WARN_RATIO_PARAM + " (" + mean + " + (" + variance + " * " + warn_ratio + ") = "
                            + warning_upperbound_value + ")");
        }
    }

    private void processErrorUpperbound(AnalysisResult result, double value) {
        if (!error_upperbound)
            return;

        double error_upperbound_value = mean + variance * error_ratio;
        result.addAnalysisParam("error_upperbound", error_upperbound_value);

        if (result.hasStatus())
            return;

        if (value > error_upperbound_value) {
            result.setStatus(AnalysisResult.Status.ERROR,
                    "Value (" + value + ") is higher than " + "average value from history plus variance * "
                            + ERROR_RATIO_PARAM + " (" + mean + " + (" + variance + " * " + error_ratio + ") = "
                            + error_upperbound_value + ")");
        }
    }
    
    @ToString
    @ClassNameAlias("recent-analysis")
    public static class Status_ extends StatusValue{
        private static final long serialVersionUID = 9004503470820013964L;

        public ValueHistory history;
        public double variance;
        public double mean;
        public long count;
    }

}
