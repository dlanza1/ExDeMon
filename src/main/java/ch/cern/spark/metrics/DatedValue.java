package ch.cern.spark.metrics;

import java.io.Serializable;
import java.time.Instant;

import ch.cern.spark.metrics.value.Value;

public class DatedValue implements Serializable, Comparable<DatedValue> {

    private static final long serialVersionUID = 3930338572646527289L;

    private Instant time;
    
    private Value value;

    public DatedValue(Instant time, Value value) {
        this.time = time;
        this.value = value;
    }

    public Value getValue() {
        return value;
    }

    public Instant getInstant() {
        return time;
    }

    @Override
    public String toString() {
        return "DatedValue [time=" + getInstant() + ", value=" + value + "]";
    }

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((time == null) ? 0 : time.hashCode());
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
		DatedValue other = (DatedValue) obj;
		if (time == null) {
			if (other.time != null)
				return false;
		} else if (!time.equals(other.time))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

	@Override
	public int compareTo(DatedValue other) {
		return time.compareTo(other.time);
	}
    
}
