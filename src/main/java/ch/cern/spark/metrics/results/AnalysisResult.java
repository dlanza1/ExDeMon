package ch.cern.spark.metrics.results;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.MonitorIDMetricIDs;
import ch.cern.spark.metrics.monitors.Monitor;
import ch.cern.spark.metrics.value.ExceptionValue;
import ch.cern.spark.metrics.value.Value;

public class AnalysisResult implements Serializable {

    private static final long serialVersionUID = -5307381437257371176L;

    public enum Status {OK, WARNING, ERROR, EXCEPTION};
    
    public Instant analysis_timestamp;
    
    public Metric analyzed_metric;
    
    public Status status;
    public String status_reason;
    
    private Map<String, Object> monitor_params;
    
    public AnalysisResult() {
        analysis_timestamp = Instant.now();
        monitor_params = new HashMap<String, Object>();
    }

    public void setAnalyzedMetric(Metric metric) {
    		if(metric.getValue().getAsException().isPresent()) {
    			this.status = Status.EXCEPTION;
    			this.status_reason = metric.getValue().getAsException().get();
    		}
    			
        this.analyzed_metric = metric;
    }

    public void setStatus(Status status, String reason) {
        this.status = status;
        this.status_reason = reason;
    }

    public Status getStatus() {
        return status;
    }

    public void addMonitorParam(String key, Object value) {
        monitor_params.put(key, value);
    }

    public boolean hasStatus() {
        return status != null;
    }
    
    public Instant getAnalysisTimestamp(){
        return analysis_timestamp;
    }
    
    public Metric getAnalyzedMetric(){
        return analyzed_metric;
    }
    
    public String getStatusReason() {
        return status_reason;
    }
    
    public static AnalysisResult buildWithStatus(Status status, String reason){
        AnalysisResult result = new AnalysisResult();
        
        result.setStatus(status, reason);
        
        return result;
    }
    
    @Override
    public String toString() {
        return "AnalysisResult [analysis_timestamp=" + analysis_timestamp + ", analyzed_metric=" + analyzed_metric
                + ", status=" + status + ", status_reason=" + status_reason + ", monitor_params=" + monitor_params
                + "]";
    }

    public static AnalysisResult buildTimingOut(MonitorIDMetricIDs ids, Monitor monitor, Instant time) {
        AnalysisResult result = new AnalysisResult(); 
        
        Value value = new ExceptionValue("Metric has timmed out.");
		result.setAnalyzedMetric(new Metric(time, value , ids.getMetricIDs()));
		
        result.addMonitorParam("name", ids.getMonitorID());
        
        return result;
    }

    public Map<String, Object> getMonitorParams() {
        return monitor_params;
    }
    
	public void setTags(Map<String, String> tags) {
		monitor_params.put("tags", tags);
	}
    
    @SuppressWarnings("unchecked")
	public Map<String, String> getTags() {
    		Object tags = monitor_params.get("tags");
    	
    		if(tags instanceof HashMap)
	    		return (Map<String, String>) monitor_params.get("tags");
    		else
    			return new HashMap<>();
	}
    
	public<R> Optional<R> map(Function<AnalysisResult, ? extends R> mapper) {
        Objects.requireNonNull(mapper);

        return Optional.ofNullable(mapper.apply(this));
	}
    
}
