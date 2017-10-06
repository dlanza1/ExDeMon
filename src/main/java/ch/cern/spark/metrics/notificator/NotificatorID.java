package ch.cern.spark.metrics.notificator;

import java.io.Serializable;
import java.util.Map;

public class NotificatorID implements Serializable{

    private static final long serialVersionUID = -4289498306145284346L;

    private String monitorID;
    
    private String notificatorID;
    
    private Map<String, String> metricIDs;
    
    public NotificatorID(String monitorID, String notificatorID, Map<String, String> metricIDs){
        this.monitorID = monitorID;
        this.notificatorID = notificatorID;
        this.metricIDs = metricIDs;
    }

    public String getMonitorID() {
        return monitorID;
    }
    
    public String getNotificatorID() {
        return notificatorID;
    }
    
    public Map<String, String> getMetricIDs(){
        return metricIDs;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((metricIDs == null) ? 0 : metricIDs.hashCode());
        result = prime * result + ((monitorID == null) ? 0 : monitorID.hashCode());
        result = prime * result + ((notificatorID == null) ? 0 : notificatorID.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        NotificatorID other = (NotificatorID) obj;
        if (metricIDs == null) {
            if (other.metricIDs != null)
                return false;
        } else if (!metricIDs.equals(other.metricIDs))
            return false;
        if (monitorID == null) {
            if (other.monitorID != null)
                return false;
        } else if (!monitorID.equals(other.monitorID))
            return false;
        if (notificatorID == null) {
            if (other.notificatorID != null)
                return false;
        } else if (!notificatorID.equals(other.notificatorID))
            return false;
        return true;
    }
    
    
}
