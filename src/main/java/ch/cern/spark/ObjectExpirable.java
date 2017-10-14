package ch.cern.spark;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

/**
 * Represents an object which is not serialized, so it needs to be reloaded in every batch
 * 
 * @author dlanza
 *
 * @param <T> Object type
 */
public abstract class ObjectExpirable<T> {

    private Duration max_life_time; 
    
    private transient Instant loadTime;
    
	private transient T object;
	
	protected ObjectExpirable(){
		max_life_time = null;
	}
	
	protected ObjectExpirable(Duration max_life_time){
	    this.max_life_time = max_life_time;
    }
	
	public final T get() throws IOException{
	    if(object != null && hasExpired())
	        object = null;
	    
		if(object == null)
			object = load();
		
		return object;
	}

	private T load() throws IOException {
	    loadTime = Instant.now();;
	    
        return loadObject();
    }

    private boolean hasExpired() {
    		if(max_life_time == null)
    			return false;
    	
        Instant currentTime = Instant.now();
        
	    Duration lifeTime = Duration.between(loadTime, currentTime).abs();
	    
        return lifeTime.compareTo(max_life_time) > 1;
    }

    protected abstract T loadObject() throws IOException;
	
}
