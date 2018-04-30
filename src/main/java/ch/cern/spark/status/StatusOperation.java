package ch.cern.spark.status;

import java.io.Serializable;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;

@ToString
@EqualsAndHashCode(callSuper=false)
public class StatusOperation<K, V> implements Serializable{

    private static final long serialVersionUID = 1246832289612522256L;
    
    public enum Op {UPDATE, LIST, REMOVE}
    @NonNull
    private Op op;
    
    @NonNull
    private K key;
    
    private V value;
    
    public StatusOperation(K key, Op op) {
        this.key = key;
        this.op = op;
    }

    public StatusOperation(K key, @NonNull V value) {
        this.op = Op.UPDATE;
        this.key = key;
        this.value = value;
    }
    
    public K getKey() {
        return key;
    }
    
    public V getValue() {
        return value;
    }

    public Op getOp() {
        return op;
    }
    
}
