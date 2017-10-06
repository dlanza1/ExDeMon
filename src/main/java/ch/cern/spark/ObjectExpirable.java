package ch.cern.spark;

import java.io.IOException;

/**
 * Represents an object which is not serialized, so it needs to be reloaded in every batch
 * 
 * @author dlanza
 *
 * @param <T> Object type
 */
public abstract class ObjectExpirable<T> {

    private int max_life_time_in_seconds; 
    
    private transient long loadTime;
    
	private transient T object;
	
	protected ObjectExpirable(){
	}
	
	protected ObjectExpirable(int max_life_time_in_seconds){
	    this.max_life_time_in_seconds = max_life_time_in_seconds;
    }
	
	public final T get() throws IOException{
	    if(object != null && hasExpired())
	        object = null;
	    
		if(object == null)
			object = load();
		
		return object;
	}

	private T load() throws IOException {
	    loadTime = System.currentTimeMillis() / 1000;
	    
        return loadObject();
    }

    private boolean hasExpired() {
        long currentTime = System.currentTimeMillis() / 1000;
        
	    long lifeTime = currentTime - loadTime;
	    
        return lifeTime > max_life_time_in_seconds;
    }

    protected abstract T loadObject() throws IOException;
	
}
