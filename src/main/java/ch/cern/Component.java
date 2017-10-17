package ch.cern;

import java.io.Serializable;
import java.util.Optional;

import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.store.Store;

public abstract class Component implements Serializable {
    
    private static final long serialVersionUID = -2299173239147440553L;
    
    public enum Type {PROPERTIES_SOURCE,
    						METRIC_SOURCE, 
    						PRE_ANALYSIS, 
    						ANAYLSIS, 
    						ANALYSIS_RESULTS_SINK, 
    						NOTIFICATOR, 
    						NOTIFICATIONS_SINK};
    private Type type;
    
    private Class<? extends Component> subClass;
    
    private String name;
    
    public Component(Type type){
        this.type = type;
    }
    
    public Component(Type type, Class<? extends Component> subClass, String name){
        this.type = type;
        this.subClass = subClass;
        this.name = name;
    }
    
    public void config(Properties properties) throws ConfigurationException {
    }

    public Type getType(){
        return type;
    }
    
    public Class<? extends Component> getSubClass(){
        return subClass;
    }
    
    public String getName(){
        return name;
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
