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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ToString
@EqualsAndHashCode(callSuper=false)
public class Metric implements Serializable{

    private static final long serialVersionUID = -182236104179624396L;

    @Getter @Setter
    private Map<String, String> attributes;
    
    @Getter
    private Instant timestamp;
    
    @Getter
    private Value value;

    public Metric(Instant timestamp, float value, Map<String, String> ids){
        this(timestamp, new FloatValue(value), ids);
    }
    
    public Metric(Instant timestamp, Value value, Map<String, String> ids){
        if(ids == null)
            this.attributes = new HashMap<String, String>();
        else
            this.attributes = new HashMap<String, String>(ids);
        
        this.timestamp = timestamp;
        this.value = value;
    }
    
    public void addAttribute(String key, String value){
        attributes.put(key, value);
    }

	public void removeAttributes(Set<String> keySet) {
		keySet.forEach(key -> attributes.remove(key));
	}

    @Override
	public Metric clone() throws CloneNotSupportedException {
    		return new Metric(timestamp, value, new HashMap<>(attributes));
    }

	public<R> Optional<R> map(Function<Metric, ? extends R> mapper) {
        Objects.requireNonNull(mapper);

        return Optional.ofNullable(mapper.apply(this));
	}

}
