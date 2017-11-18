package ch.cern.spark.metrics;

import java.io.Serializable;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;

public class Metric implements Serializable{

    private static final long serialVersionUID = -182236104179624396L;

    private Map<String, String> ids;
    
    private Instant timestamp;
    
    private Value value;

    public Metric(Instant timestamp, float value, Map<String, String> ids){
        this(timestamp, new FloatValue(value), ids);
    }
    
    public Metric(Instant timestamp, Value value, Map<String, String> ids){
        if(ids == null)
            this.ids = new HashMap<String, String>();
        else
            this.ids = new HashMap<String, String>(ids);
        
        this.timestamp = timestamp;
        this.value = value;
    }
    
    public void addID(String key, String value){
        ids.put(key, value);
    }
    
    public void setIDs(Map<String, String> ids) {
        this.ids = ids;
    }

	public void removeIDs(Set<String> keySet) {
		keySet.forEach(key -> ids.remove(key));
	}
    
    public Map<String, String> getIDs(){
        return ids;
    }
    
    public void setTimestamp(Instant timestamp) {
        this.timestamp = timestamp;
    }
    
    public Instant getInstant(){
        return timestamp;
    }

    public Value getValue() {
        return value;
    }
    
    public void setValue(Value newValue) {
        this.value = newValue;
    }

    @Override
    public String toString() {
        return "Metric [ids=" + ids + ", timestamp=" + timestamp + ", value=" + value + "]";
    }
    
    @Override
	public Metric clone() throws CloneNotSupportedException {
    		return new Metric(timestamp, value, new HashMap<>(ids));
    }
    
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((ids == null) ? 0 : ids.hashCode());
		result = prime * result + ((timestamp == null) ? 0 : timestamp.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
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
		Metric other = (Metric) obj;
		if (ids == null) {
			if (other.ids != null)
				return false;
		} else if (!ids.equals(other.ids))
			return false;
		if (timestamp == null) {
			if (other.timestamp != null)
				return false;
		} else if (timestamp.getEpochSecond() != other.timestamp.getEpochSecond())
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	public<R> Optional<R> map(Function<Metric, ? extends R> mapper) {
        Objects.requireNonNull(mapper);

        return Optional.ofNullable(mapper.apply(this));
	}

}
