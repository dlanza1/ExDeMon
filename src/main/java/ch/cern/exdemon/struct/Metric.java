package ch.cern.exdemon.struct;

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
    
    public static class Value implements Serializable {
        
        private static final long serialVersionUID = -23682055119611586L;
        
        private String str = null;
        private Boolean bool = null;
        private Double num = null;
        
        public Value() {
        }

        public String getStr() {
            return str;
        }

        public void setStr(String str) {
            this.str = str;
        }

        public Boolean getBool() {
            return bool;
        }

        public void setBool(Boolean bool) {
            this.bool = bool;
        }

        public Double getNum() {
            return num;
        }

        public void setNum(Double num) {
            this.num = num;
        }

    }
    
}
