package ch.cern.spark.metrics.notifications;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import ch.cern.Taggable;

public class Notification implements Serializable, Taggable {
    
    private static final long serialVersionUID = 6730655599755849423L;
    
    private Instant notification_timestamp;
    
    private String monitor_id;
    
    private String notificator_id;
    
    private Map<String, String> metric_ids;
    
    private String reason;

	private Map<String, String> tags;

	private Set<String> sinks;

    public Notification(Instant timestamp, String monitorID, String notificatorID, Map<String, String> metricIDs,
            String reason, Set<String> sinks) {
        this.notification_timestamp = timestamp;
        this.monitor_id = monitorID;
        this.notificator_id = notificatorID;
        this.metric_ids = metricIDs;
        this.reason = reason;
        this.sinks = sinks;
        
        tags = replaceMetricAttributesInTags(tags);
    }

    public Notification() {
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public Instant getTimestamp() {
        return notification_timestamp;
    }

    public String getMonitorID() {
        return monitor_id;
    }

    public String getNotificatorID() {
        return notificator_id;
    }

    public Map<String, String> getMetricIDs() {
        return metric_ids;
    }

    public String getReason() {
        return reason;
    }

    public void setTimestamp(Instant timestamp) {
        this.notification_timestamp = timestamp;
    }

    public void setMonitorID(String monitorID) {
        this.monitor_id = monitorID;
    }

    public void setNotificatorID(String notificatorID) {
        this.notificator_id = notificatorID;
    }

    public void setMetricIDs(Map<String, String> metricIDs) {
        this.metric_ids = metricIDs;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

	public void setTags(Map<String, String> tags) {
		this.tags = replaceMetricAttributesInTags(tags);
	}
    
    private Map<String, String> replaceMetricAttributesInTags(Map<String, String> tags) {
    		if(metric_ids == null)
    			return tags;
    	
    		HashMap<String, String> newTags = new HashMap<>(tags);
		newTags.entrySet().stream().filter(entry -> entry.getValue().startsWith("%")).forEach(entry -> {
			String metricKey = entry.getValue().substring(1);
			String metricValue = metric_ids.get(metricKey);
			
			if(metricValue != null)
				newTags.put(entry.getKey(), metricValue);
		});
		
		return newTags;
	}
	
	public Map<String, String> getTags() {
		return tags;
	}

	public Set<String> getSinkIds() {
		return sinks;
	}
	
	public void setSinkIds(Set<String> sinks) {
		this.sinks = sinks;
	}

	@Override
	public String toString() {
		return "Notification [timestamp=" + notification_timestamp + ", monitorID=" + monitor_id + ", notificatorID=" + notificator_id
				+ ", metricIDs=" + metric_ids + ", reason=" + reason + ", tags=" + tags + ", sinks=" + sinks + "]";
	}
	
}
