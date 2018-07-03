package ch.cern.exdemon.monitor;

import java.util.Map;

import ch.cern.spark.status.IDStatusKey;
import ch.cern.spark.status.storage.ClassNameAlias;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ClassNameAlias("monitor-key")
@ToString
@EqualsAndHashCode(callSuper=false)
public class MonitorStatusKey implements IDStatusKey{
    
    private static final long serialVersionUID = -7288054142035095969L;

    private String id;
    
    @Getter
    private Map<String, String> metric_attributes;
    
    public MonitorStatusKey(String monitorID, Map<String, String> metricIDs){
        this.id = monitorID;
        this.metric_attributes = metricIDs;
    }

    public String getID() {
        return id;
    }

}
