package ch.cern.spark.metrics;

import java.io.Serializable;
import java.util.Date;

public class DatedValue implements Serializable {

    private static final long serialVersionUID = 3930338572646527289L;

    private int timeInSeconds;
    
    private float value;

    public DatedValue(int timeInSeconds, float value) {
        this.timeInSeconds = timeInSeconds;
        this.value = value;
    }

    public int getTimeInSeconds() {
        return timeInSeconds;
    }

    public float getValue() {
        return value;
    }

    public Date getDate() {
        return new Date(timeInSeconds * 1000l);
    }

    @Override
    public String toString() {
        return "DatedValue [time=" + getDate() + ", value=" + value + "]";
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + timeInSeconds;
        result = prime * result + Float.floatToIntBits(value);
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
        if (timeInSeconds != other.timeInSeconds)
            return false;
        if (Float.floatToIntBits(value) != Float.floatToIntBits(other.value))
            return false;
        return true;
    }
    
}
