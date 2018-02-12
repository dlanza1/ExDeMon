package ch.cern.spark.metrics.results;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

import ch.cern.Taggable;
import ch.cern.spark.metrics.Metric;
import lombok.Getter;
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
    
    @Setter
    private Instant analysis_timestamp;
    
    @Getter
    private Metric analyzed_metric;
    
    private Map<String, Object> analysis_params;
    
    private Map<String, String> tags;
    
    public AnalysisResult() {
        analysis_timestamp = Instant.now();
        analysis_params = new HashMap<String, Object>();
        tags = new HashMap<>();
    }

	public void setAnalyzedMetric(Metric metric) {
    		if(metric.getValue().getAsException().isPresent()) {
    			this.status = Status.EXCEPTION;
    			this.status_reason = metric.getValue().getAsException().get();
    		}
    			
        this.analyzed_metric = metric;
        
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
    
	public<R> Optional<R> map(Function<AnalysisResult, ? extends R> mapper) {
        Objects.requireNonNull(mapper);

        return Optional.ofNullable(mapper.apply(this));
	}
    
}
