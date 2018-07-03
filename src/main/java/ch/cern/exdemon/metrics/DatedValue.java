package ch.cern.exdemon.metrics;

import java.io.Serializable;
import java.time.Instant;

import ch.cern.exdemon.metrics.value.Value;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@EqualsAndHashCode(callSuper=false)
@ToString
public class DatedValue implements Serializable, Comparable<DatedValue> {

    private static final long serialVersionUID = 3930338572646527289L;

    @Getter
    private Instant time;
    
    @Getter
    private Value value;

    public DatedValue(Instant time, Value value) {
        this.time = time;
        this.value = value;
    }
    
	@Override
	public int compareTo(DatedValue other) {
		return time.compareTo(other.time);
	}
    
}
