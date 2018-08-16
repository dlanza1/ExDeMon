package ch.cern.spark.status;

import java.io.Serializable;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

import lombok.Getter;

public abstract class StatusValue implements Serializable {

	private static final long serialVersionUID = -2362831624128266105L;
	
	@Getter
	private long status_update_time = 0;
	
	public StatusValue() {
	    status_update_time = 0;
	}
	
	@SuppressWarnings("unchecked")
	public<T> void update(State<T> status, Time time) {
		status_update_time = time.milliseconds();
		
		status.update((T) this);
	}

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (status_update_time ^ (status_update_time >>> 32));
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
        StatusValue other = (StatusValue) obj;
        if (status_update_time != other.status_update_time)
            return false;
        return true;
    }

}
