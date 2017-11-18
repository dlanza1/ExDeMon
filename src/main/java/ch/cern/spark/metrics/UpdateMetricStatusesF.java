package ch.cern.spark.metrics;

import java.time.Instant;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.monitors.Monitor;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.store.Store;

public class UpdateMetricStatusesF
        implements Function4<Time, MonitorIDMetricIDs, Optional<Metric>, State<Store>, Optional<AnalysisResult>> {

    private static final long serialVersionUID = 3156649511706333348L;
    
    public static String DATA_EXPIRATION_PARAM = "data.expiration";
    public static java.time.Duration DATA_EXPIRATION_DEFAULT = java.time.Duration.ofHours(3);

    private Properties propertiesSourceProperties;
    
    public UpdateMetricStatusesF(Properties propertiesSourceProperties) {
    		this.propertiesSourceProperties = propertiesSourceProperties;
    }
    
    @Override
    public Optional<AnalysisResult> call(
            Time time, MonitorIDMetricIDs ids, Optional<Metric> metricOpt, State<Store> storeState) 
            throws Exception {
    	
    		Monitors.initCache(propertiesSourceProperties);
        
        Optional<Monitor> monitorOpt = Optional.ofNullable(Monitors.getCache().get().get(ids.getMonitorID()));
        if(!monitorOpt.isPresent()) {
        		storeState.remove();
	        	return Optional.empty();
        }
        Monitor monitor = monitorOpt.get();
        
        if(storeState.isTimingOut())
            return Optional.of(AnalysisResult.buildTimingOut(ids, monitor, Instant.ofEpochMilli(time.milliseconds())));
        
        if(!metricOpt.isPresent())
            return Optional.absent();
        
        Metric metric = metricOpt.get();

        java.util.Optional<AnalysisResult> result = monitor.process(storeState, metric);
        if(result.isPresent()) {
        		result.get().setAnalyzedMetric(metric);
            
            return toOptinal(result);
        }else{
        		return Optional.empty();
        }
    }

	private Optional<AnalysisResult> toOptinal(java.util.Optional<AnalysisResult> result) {
		return result.isPresent() ? Optional.of(result.get()) : Optional.empty();
	}

}
