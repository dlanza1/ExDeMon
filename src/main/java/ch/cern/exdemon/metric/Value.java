package ch.cern.exdemon.metric;

import java.io.Serializable;

public class Value implements Serializable {
    
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
