package ch.cern.exdemon.metric;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Map;

public class Metric implements Serializable {
    
    private static final long serialVersionUID = -2090504797500130224L;
    
    private Map<String, String> att;
    private Timestamp timestamp;
    private Value value;

    public Metric() {
    }

    public Map<String, String> getAtt() {
        return att;
    }

    public void setAttributes(Map<String, String> att) {
        this.att = att;
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Timestamp timestamp) {
        this.timestamp = timestamp;
    }

    public Value getValue() {
        return value;
    }

    public void setValue(Value value) {
        this.value = value;
    }
    
}
