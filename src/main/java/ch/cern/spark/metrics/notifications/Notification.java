package ch.cern.spark.metrics.notifications;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;

public class Notification implements Serializable {
    
    private static final long serialVersionUID = 6730655599755849423L;
    
    private Date timestamp;
    
    private String monitorID;
    
    private String notificatorID;
    
    private Map<String, String> metricIDs;
    
    private String reason;

    public Notification(Date timestamp, String monitorID, String notificatorID, Map<String, String> metricIDs,
            String reason) {
        this.timestamp = timestamp;
        this.monitorID = monitorID;
        this.notificatorID = notificatorID;
        this.metricIDs = metricIDs;
        this.reason = reason;
    }

    public Notification() {
    }

    public static long getSerialversionuid() {
        return serialVersionUID;
    }

    public Date getTimestamp() {
        return timestamp;
    }

    public String getMonitorID() {
        return monitorID;
    }

    public String getNotificatorID() {
        return notificatorID;
    }

    public Map<String, String> getMetricIDs() {
        return metricIDs;
    }

    public String getReason() {
        return reason;
    }

    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }

    public void setMonitorID(String monitorID) {
        this.monitorID = monitorID;
    }

    public void setNotificatorID(String notificatorID) {
        this.notificatorID = notificatorID;
    }

    public void setMetricIDs(Map<String, String> metricIDs) {
        this.metricIDs = metricIDs;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }
    
}
