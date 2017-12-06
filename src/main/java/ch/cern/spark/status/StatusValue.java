package ch.cern.spark.status;

import java.io.Serializable;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

public abstract class StatusValue implements Serializable {

	private static final long serialVersionUID = -2362831624128266105L;
	
	private long status_update_time = 0;
	
	public long getUpdatedTime(){
		return status_update_time;
	}
	
	@SuppressWarnings("unchecked")
	public<T> void update(State<T> status, Time time) {
		status_update_time = time.milliseconds();
		
		status.update((T) this);
	}

}
