package ch.cern.spark.metrics;

import java.io.Serializable;
import java.util.Map;

public class MonitorIDMetricIDs implements Serializable{
    
    private static final long serialVersionUID = -7288054142035095969L;

    private String monitorID;
    
    private Map<String, String> metricIDs;
    
    public MonitorIDMetricIDs(String monitorID, Map<String, String> metricIDs){
        this.monitorID = monitorID;
        this.metricIDs = metricIDs;
    }

    public String getMonitorID() {
        return monitorID;
    }
    
    public Map<String, String> getMetricIDs(){
        return metricIDs;
    }

    @Override
    public String toString() {
        return "MonitorIDMetricIDs [monitorID=" + monitorID + ", metricIDs=" + metricIDs + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((metricIDs == null) ? 0 : metricIDs.hashCode());
        result = prime * result + ((monitorID == null) ? 0 : monitorID.hashCode());
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
        MonitorIDMetricIDs other = (MonitorIDMetricIDs) obj;
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
        return true;
    }

}
