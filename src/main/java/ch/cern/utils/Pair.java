package ch.cern.utils;

import java.io.Serializable;

import lombok.EqualsAndHashCode;
import lombok.ToString;

@ToString
@EqualsAndHashCode(callSuper=false)
public class Pair<K, V> implements Serializable{

    private static final long serialVersionUID = 4231657422199019843L;

    public K first;
    
    public V second;
    
    public Pair(K first, V second){
        this.first = first;
        this.second = second;
    }

    public K first() {
    		return first;
    }
    
    public V second() {
		return second;
    }
    
}
