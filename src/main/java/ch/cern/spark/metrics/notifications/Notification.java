package ch.cern.spark.metrics.notifications;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

import ch.cern.Taggable;
import ch.cern.spark.metrics.results.AnalysisResult;
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

    @Setter @NonNull
	private Map<String, String> tags;

    @Getter @Setter @NonNull
	private Set<String> sink_ids;

    @Getter @NonNull
    private AnalysisResult triggeringResult;

    public Notification(
            @NonNull Instant timestamp, 
            @NonNull String monitorID, 
            @NonNull String notificatorID, 
            @NonNull Map<String, String> metric_attributes,
            @NonNull String reason, 
            @NonNull Set<String> sinks,
            @NonNull Map<String, String> tags,
            @NonNull AnalysisResult triggeringResult) {
        this.notification_timestamp = timestamp;
        this.monitor_id = monitorID;
        this.notificator_id = notificatorID;
        this.metric_attributes = metric_attributes;
        this.reason = reason;
        this.sink_ids = sinks;
        this.tags = tags;
        this.triggeringResult = triggeringResult;
    }
    
    @Override
    public Map<String, String> getTags() {
        return tags;
    }
	
}
