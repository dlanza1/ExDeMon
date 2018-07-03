package ch.cern.exdemon.monitor.trigger;

import java.util.Map;

import ch.cern.spark.status.IDStatusKey;
import ch.cern.spark.status.storage.ClassNameAlias;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@ClassNameAlias("trigger-key")
@ToString
@EqualsAndHashCode(callSuper=false)
public class TriggerStatusKey implements IDStatusKey{

    private static final long serialVersionUID = -4289498306145284346L;

    @NonNull
    private String id;
    
    @Getter @NonNull
    private String monitor_id;
    
    @Getter @NonNull
    private Map<String, String> metric_attributes;
    
    public TriggerStatusKey(@NonNull String monitorID, @NonNull String triggerID, @NonNull Map<String, String> metric_attributes){
        this.monitor_id = monitorID;
        this.id = triggerID;
        this.metric_attributes = metric_attributes;
    }
    
    public String getNotificatorID() {
        return id;
    }
    
    @Override
    public String getID() {
        return monitor_id + ":" + id;
    }
    
}
