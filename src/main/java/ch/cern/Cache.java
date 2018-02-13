package ch.cern;

import java.time.Duration;
import java.time.Instant;

import org.apache.log4j.Logger;

public abstract class Cache<T> {
	
    private transient final static Logger LOG = Logger.getLogger(Cache.class.getName());
    
	private transient T object = null;
	private transient T previousObject;
	
	private Duration expirationPeriod = null;
	
	private Instant lastLoadTime = null;
	
	public synchronized T get() throws Exception {
		Instant currentTime = Instant.now();
		
	    if(object != null && hasExpired(currentTime)) {
	        previousObject = object;
	    		object = null;
	    }
	    
		if(object == null)
		    try {
		        object = loadCache(currentTime);
		    }catch(Exception e) {
		        if(inErrorGetPrevious() && previousObject != null) {
		            LOG.error("Error when loading cache. Loading previous object...", e);
		            
		            return previousObject;
		        }else {
		            throw e;
		        }
		    }
		
		return object;
	}
	
	protected T loadCache(Instant currentTime) throws Exception {
	    lastLoadTime = currentTime;
	    
        return load();
    }

	protected boolean hasExpired(Instant currentTime) {
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
    
    protected boolean inErrorGetPrevious() {
        return false;
    }
    
    public T getPreviousObject() {
        return previousObject;
    }

	public Duration getExpirationPeriod() {
		return expirationPeriod;
	}

}
