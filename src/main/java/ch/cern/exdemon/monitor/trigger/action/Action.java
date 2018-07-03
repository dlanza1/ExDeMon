package ch.cern.exdemon.monitor.trigger.action;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Set;

import ch.cern.Taggable;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import lombok.ToString;

@ToString
public class Action implements Serializable, Taggable {
    
    private static final long serialVersionUID = 6730655599755849423L;
    
    @Getter @Setter @NonNull
    private Instant creation_timestamp;
    
    @Getter @Setter @NonNull
    private String monitor_id;
    
    @Getter @Setter @NonNull
    private String trigger_id;
    
    @Getter @Setter @NonNull
    private Map<String, String> metric_attributes;
    
    @Getter @Setter @NonNull
    private String reason;

    @Setter @NonNull
	private Map<String, String> tags;

    @Getter @Setter @NonNull
	private Set<String> actuatorIDs;

    @Getter @Setter @NonNull
    private AnalysisResult triggeringResult;

    public Action(
            @NonNull String monitorID, 
            @NonNull String trgiggerID, 
            @NonNull Map<String, String> metric_attributes,
            @NonNull String reason, 
            @NonNull Set<String> actuatorIDs,
            @NonNull Map<String, String> tags,
            @NonNull AnalysisResult triggeringResult) {
        this.creation_timestamp = Instant.now();
        this.monitor_id = monitorID;
        this.trigger_id = trgiggerID;
        this.metric_attributes = metric_attributes;
        this.reason = reason;
        this.actuatorIDs = actuatorIDs;
        this.tags = tags;
        this.triggeringResult = triggeringResult;
    }
    
    @Override
    public Map<String, String> getTags() {
        return tags;
    }
	
}
