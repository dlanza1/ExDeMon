package ch.cern.components;

import java.io.Serializable;
import java.util.Optional;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.HasStatus;
import ch.cern.spark.status.StatusValue;

public abstract class Component implements Serializable {
    
    private static final long serialVersionUID = -2299173239147440553L;
    
    public enum Type {STATUS_STORAGE,
    						PROPERTIES_SOURCE,
    						METRIC_SOURCE, 
    						AGGREGATION,
    						ANAYLSIS, 
    						ANALYSIS_RESULTS_SINK, 
    						TRIGGER, 
    						ACTUATOR};
    						
    	private String id;
    	
    public String getId() {
    		return id;
    	}

    	public void setId(String id) {
    		this.id = id;
    	}
    
    public void config(Properties properties) throws ConfigurationException {
    }

    public boolean hasStatus() {
    		return this instanceof HasStatus;
    }
    
	public Optional<StatusValue> getStatus() {
		if(hasStatus())
			return Optional.ofNullable(((HasStatus) this).save());
		else
			return Optional.empty();
	}
    
}
