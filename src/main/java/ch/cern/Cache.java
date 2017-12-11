package ch.cern;

import java.time.Duration;
import java.time.Instant;

public abstract class Cache<T> {
	
	private transient T object = null;
	
	private Duration expirationPeriod = null;
	
	private Instant lastLoadTime = null;
	
	public synchronized T get() throws Exception {
		Instant currentTime = Instant.now();
		
	    if(object != null && hasExpired(currentTime))
	    		object = null;
	    
		if(object == null)
			object = loadCache(currentTime);
		
		return object;
	}
	
	private T loadCache(Instant currentTime) throws Exception {
	    lastLoadTime = currentTime;
	    
        return load();
    }

    public boolean hasExpired(Instant currentTime) {
    		if(expirationPeriod == null)
    			return false;
    		
    		if(lastLoadTime == null)
    			return true;
    		
	    Duration lifeTime = Duration.between(lastLoadTime, currentTime).abs();
	    
        return lifeTime.compareTo(expirationPeriod) > 0;
    }
    
    public void setExpiration(Duration expirationPeriod) {
		this.expirationPeriod = expirationPeriod;
	}
    
    public void set(T newObject) {
    		lastLoadTime = Instant.now();
    		
    		this.object = newObject;
    }
    
	public void reset() {
		lastLoadTime = null;
		object = null;
	}
    
    protected abstract T load() throws Exception;

	public Duration getExpirationPeriod() {
		return expirationPeriod;
	}

}
