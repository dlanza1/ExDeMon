package ch.cern.spark.status;

import java.io.Serializable;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

import lombok.Getter;

public abstract class StatusValue implements Serializable {

	private static final long serialVersionUID = -2362831624128266105L;
	
	@Getter
	private long status_update_time = 0;
	
	@SuppressWarnings("unchecked")
	public<T> void update(State<T> status, Time time) {
		status_update_time = time.milliseconds();
		
		status.update((T) this);
	}

}
