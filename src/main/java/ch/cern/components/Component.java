package ch.cern.components;

import java.io.Serializable;
import java.util.Optional;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.store.Store;

public abstract class Component implements Serializable {
    
    private static final long serialVersionUID = -2299173239147440553L;
    
    public enum Type {PROPERTIES_SOURCE,
    						METRIC_SOURCE, 
    						ANAYLSIS, 
    						ANALYSIS_RESULTS_SINK, 
    						NOTIFICATOR, 
    						NOTIFICATIONS_SINK};
    						
    	private String id;
    	
    public String getId() {
    		return id;
    	}

    	public void setId(String id) {
    		this.id = id;
    	}
    
    public void config(Properties properties) throws ConfigurationException {
    }

    public boolean hasStore() {
    		return this instanceof HasStore;
    }
    
	public Optional<Store> getStore() {
		if(hasStore())
			return Optional.ofNullable(((HasStore) this).save());
		else
			return Optional.empty();
	}
    
}
