package ch.cern.spark.metrics.notifications;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import ch.cern.Taggable;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
public class Notification implements Serializable, Taggable {
    
    private static final long serialVersionUID = 6730655599755849423L;
    
    @Getter @Setter
    private Instant notification_timestamp;
    
    @Getter @Setter
    private String monitor_id;
    
    @Getter @Setter
    private String notificator_id;
    
    @Getter @Setter
    private Map<String, String> metric_attributes;
    
    @Getter @Setter
    private String reason;

	private Map<String, String> tags;

    @Getter @Setter
	private Set<String> sink_ids;

    public Notification(Instant timestamp, String monitorID, String notificatorID, Map<String, String> metric_attributes,
            String reason, Set<String> sinks) {
        this.notification_timestamp = timestamp;
        this.monitor_id = monitorID;
        this.notificator_id = notificatorID;
        this.metric_attributes = metric_attributes;
        this.reason = reason;
        this.sink_ids = sinks;
        
        tags = replaceMetricAttributesInTags(tags);
    }

    public Notification() {
    }

	public void setTags(Map<String, String> tags) {
		this.tags = replaceMetricAttributesInTags(tags);
	}
    
    private Map<String, String> replaceMetricAttributesInTags(Map<String, String> tags) {
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
