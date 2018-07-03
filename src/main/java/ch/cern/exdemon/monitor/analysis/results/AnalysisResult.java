package ch.cern.exdemon.monitor.analysis.results;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import ch.cern.Taggable;
import ch.cern.exdemon.metrics.Metric;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@ToString
public class AnalysisResult implements Serializable, Taggable {

    private static final long serialVersionUID = -5307381437257371176L;

    public enum Status {OK, WARNING, ERROR, EXCEPTION};
    @Getter
    private Status status;
    @Getter
    private String status_reason;
    
    @Getter @Setter
    private long timestamp; //corresponding to triggering metric
    
    @Getter @Setter @NonNull
    private Instant analysis_timestamp; //object creation
    
    @Getter @NonNull
    private Metric analyzed_metric;
    
    @NonNull
    private Map<String, Object> analysis_params;
    
    @NonNull
    private Map<String, String> tags;
    
    public AnalysisResult() {
        analysis_timestamp = Instant.now();
        timestamp = analysis_timestamp.toEpochMilli();
        analysis_params = new HashMap<String, Object>();
        tags = new HashMap<>();
    }

	public void setAnalyzedMetric(@NonNull Metric metric) {
        if (metric.getValue().getAsException().isPresent()) {
            this.status = Status.EXCEPTION;
            this.status_reason = metric.getValue().getAsException().get();
        }
    			
        this.analyzed_metric = metric;
        this.timestamp = metric.getTimestamp().toEpochMilli();
        
        tags = replaceMetricAttributesInTags(tags);
    }

    public void setStatus(Status status, String reason) {
        this.status = status;
        this.status_reason = reason;
    }

    public void addAnalysisParam(String key, Object value) {
        analysis_params.put(key, value);
    }

    public boolean hasStatus() {
        return status != null;
    }
    
    public static AnalysisResult buildWithStatus(Status status, String reason){
        AnalysisResult result = new AnalysisResult();
        
        result.setStatus(status, reason);
        
        return result;
    }

    public Map<String, Object> getAnalysisParams() {
        return analysis_params;
    }
    
	public void setTags(Map<String, String> tags) {
		this.tags = replaceMetricAttributesInTags(tags);
	}
    
    private Map<String, String> replaceMetricAttributesInTags(Map<String, String> tags) {
    		if(analyzed_metric == null || tags == null)
    			return tags;
    	
    		HashMap<String, String> newTags = new HashMap<>(tags);
		newTags.entrySet().stream().filter(entry -> entry.getValue().startsWith("%")).forEach(entry -> {
			String metricKey = entry.getValue().substring(1);
			String metricValue = analyzed_metric.getAttributes().get(metricKey);
			
			if(metricValue != null)
				newTags.put(entry.getKey(), metricValue);
		});
		
		return newTags;
	}

	public Map<String, String> getTags() {    	
    		return tags;
	}
    
}
