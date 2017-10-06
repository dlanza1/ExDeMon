package ch.cern.spark.metrics;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class Metric implements Serializable{

    private static final long serialVersionUID = -182236104179624396L;

    private Map<String, String> ids;
    
    private Date timestamp;
    
    private float value;

    public Metric(Date timestamp, float value, Map<String, String> ids){
        if(ids == null)
            this.ids = new HashMap<String, String>();
        else
            this.ids = ids;
        
        this.timestamp = timestamp;
        this.value = value;
    }
    
    public void addID(String key, String value){
        ids.put(key, value);
    }
    
    public void setIDs(Map<String, String> ids) {
        this.ids = ids;
    }
    
    public Map<String, String> getIDs(){
        return ids;
    }
    
    public void setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
    }
    
    public Date getTimestamp(){
        return timestamp;
    }

    public Float getValue() {
        return value;
    }
    
    public void setValue(float newValue) {
        this.value = newValue;
    }

    @Override
    public String toString() {
        return "Metric [ids=" + ids + ", timestamp=" + timestamp + ", value=" + value + "]";
    }

}
