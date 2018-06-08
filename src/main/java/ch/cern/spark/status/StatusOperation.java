package ch.cern.spark.status;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.api.java.function.Function;

import lombok.EqualsAndHashCode;
import lombok.NonNull;
import lombok.ToString;
import scala.Tuple2;

@ToString
@EqualsAndHashCode(callSuper=false)
public class StatusOperation<K, V> implements Serializable{

    private static final long serialVersionUID = 1246832289612522256L;
    
    private String id;
    
    public enum Op {UPDATE, LIST, REMOVE, SHOW}
    @NonNull
    private Op op;
    
    @NonNull
    private K key;
    
    private V value;

	private List<Function<Tuple2<StatusKey, StatusValue>, Boolean>> filters;
    
    public StatusOperation(K key, @NonNull V value) {
    	this.id = null;
        this.op = Op.UPDATE;
        this.key = key;
        this.value = value;
    }

    public StatusOperation(@NonNull String id, K key, @NonNull Op op) {
    	this.id = id;
        this.op = op;
        this.key = key;
        this.value = null;
    }
    
    public StatusOperation(@NonNull String id, @NonNull List<Function<Tuple2<StatusKey, StatusValue>, Boolean>> filters) {
    	this.id = id;
        this.op = Op.LIST;
        this.key = null;
        this.value = null;
        this.filters = filters;
    }
    
	public StatusOperation(
			String id, 
			Op op, 
			K key, 
			V value,
			List<Function<Tuple2<StatusKey, StatusValue>, Boolean>> filters) {
		super();
		this.id = id;
		this.op = op;
		this.key = key;
		this.value = value;
		this.filters = filters;
	}
    
    public String getId() {
		return id;
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

	public boolean filter(Tuple2<StatusKey, StatusValue> tuple) throws Exception {
		if(!op.equals(Op.LIST))
			throw new Exception("Filter is not allowed, it must be a list operation");
		
		for (Function<Tuple2<StatusKey, StatusValue>, Boolean> function : filters)
			if(!function.call(tuple))
				return false;
		
		return true;
	}

	public List<Function<Tuple2<StatusKey, StatusValue>, Boolean>> getFilters() {
		return filters;
	}
    
}
