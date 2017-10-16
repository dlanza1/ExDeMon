package ch.cern;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;

/**
 * Represents an object which is not serialized, so it needs to be reloaded in every batch
 *
 * @param <T> Object type
 */
public abstract class Cache<T> {

    private Duration max_life_time; 
    
    private transient Instant loadTime;
    
	private transient T object;
	
	protected Cache(){
		max_life_time = null;
	}
	
	protected Cache(Duration max_life_time){
	    this.max_life_time = max_life_time;
    }
	
	public final T get() throws IOException{
	    if(object != null && hasExpired())
	        object = null;
	    
		if(object == null)
			object = loadCache();
		
		return object;
	}

	private T loadCache() throws IOException {
	    loadTime = Instant.now();;
	    
        return load();
    }

    private boolean hasExpired() {
    		if(max_life_time == null)
    			return false;
    	
        Instant currentTime = Instant.now();
        
	    Duration lifeTime = Duration.between(loadTime, currentTime).abs();
	    
        return lifeTime.compareTo(max_life_time) > 1;
    }

    protected abstract T load() throws IOException;
	
}
