package ch.cern.spark.metrics.notificator;

import java.util.Map;

import ch.cern.spark.status.IDStatusKey;
import ch.cern.spark.status.storage.ClassNameAlias;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;

@ClassNameAlias("notificator-key")
@ToString
@EqualsAndHashCode(callSuper=false)
public class NotificatorStatusKey implements IDStatusKey{

    private static final long serialVersionUID = -4289498306145284346L;

    @NonNull
    private String id;
    
    @Getter @NonNull
    private String monitor_id;
    
    @Getter @NonNull
    private Map<String, String> metric_attributes;
    
    public NotificatorStatusKey(@NonNull String monitorID, @NonNull String notificatorID, @NonNull Map<String, String> metric_attributes){
        this.monitor_id = monitorID;
        this.id = notificatorID;
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
