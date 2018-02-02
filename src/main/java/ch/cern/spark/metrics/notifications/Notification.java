package ch.cern.spark.metrics.notifications;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import ch.cern.Taggable;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@ToString
public class Notification implements Serializable, Taggable {
    
    private static final long serialVersionUID = 6730655599755849423L;
    
    @Getter @Setter @NonNull
    private Instant notification_timestamp;
    
    @Getter @Setter @NonNull
    private String monitor_id;
    
    @Getter @Setter @NonNull
    private String notificator_id;
    
    @Getter @Setter @NonNull
    private Map<String, String> metric_attributes;
    
    @Getter @Setter @NonNull
    private String reason;

    @NonNull
	private Map<String, String> tags;

    @Getter @Setter @NonNull
	private Set<String> sink_ids;

    public Notification(
            @NonNull Instant timestamp, 
            @NonNull String monitorID, 
            @NonNull String notificatorID, 
            @NonNull Map<String, String> metric_attributes,
            @NonNull String reason, 
            @NonNull Set<String> sinks,
            @NonNull Map<String, String> tags) {
        this.notification_timestamp = timestamp;
        this.monitor_id = monitorID;
        this.notificator_id = notificatorID;
        this.metric_attributes = metric_attributes;
        this.reason = reason;
        this.sink_ids = sinks;
        this.tags = replaceMetricAttributesInTags(tags);
    }
    
    public void setTags(Map<String, String> tags) {
        this.tags = replaceMetricAttributesInTags(tags);
    }
    
    private Map<String, String> replaceMetricAttributesInTags(@NonNull Map<String, String> tags) {
    		if(metric_attributes == null)
    			return tags;
    	
    		HashMap<String, String> newTags = new HashMap<>(tags);
		newTags.entrySet().stream().filter(entry -> entry.getValue().startsWith("%")).forEach(entry -> {
			String metricKey = entry.getValue().substring(1);
			String metricValue = metric_attributes.get(metricKey);
			
			if(metricValue != null)
				newTags.put(entry.getKey(), metricValue);
		});
		
		return newTags;
	}

    @Override
    public Map<String, String> getTags() {
        return tags;
    }
	
}
