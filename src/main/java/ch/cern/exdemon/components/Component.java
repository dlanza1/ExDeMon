package ch.cern.exdemon.components;

import java.io.Serializable;
import java.util.Optional;

import org.apache.log4j.Logger;

import ch.cern.exdemon.metrics.defined.DefinedMetric;
import ch.cern.exdemon.metrics.schema.MetricSchema;
import ch.cern.exdemon.monitor.Monitor;
import ch.cern.exdemon.monitor.trigger.action.silence.Silence;
import ch.cern.properties.Properties;
import ch.cern.spark.status.HasStatus;
import ch.cern.spark.status.StatusValue;
import lombok.Getter;

public abstract class Component implements Serializable {
    
    private transient final static Logger LOG = Logger.getLogger(Component.class.getName());

    private static final long serialVersionUID = -2299173239147440553L;

    public enum Type {
        STATUS_STORAGE, 
        COMPONENTS_SOURCE,
        METRIC_SOURCE, 
        AGGREGATION,
        ANAYLSIS, 
        ANALYSIS_RESULTS_SINK, 
        TRIGGER,
        ACTUATOR,
        SCHEMA  { String type(){ return MetricSchema.class.getName(); }},
        METRIC  { String type(){ return DefinedMetric.class.getName(); }},
        MONITOR { String type(){ return Monitor.class.getName(); }},
        SILENCE { String type(){ return Silence.class.getName(); }};
        
        String type() { return null; }
    };

    private String id;
    
    @Getter
    private int propertiesHash;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    protected final ConfigurationResult buildConfig(Properties properties) {
        propertiesHash = properties.hashCode();
        
        try {
            return config(properties);
        }catch(Throwable e) {
            LOG.error("Exception not processed by component", e);
            
            return ConfigurationResult.SUCCESSFUL().withError("NO_PROCESSED_BY_COMPONENT", e.getClass().getSimpleName() + ": " + e.getMessage());
        }
    }

    protected ConfigurationResult config(Properties properties) {   
        return ConfigurationResult.SUCCESSFUL();
    }

    public boolean hasStatus() {
        return this instanceof HasStatus;
    }

    public Optional<StatusValue> getStatus() {
        if (hasStatus())
            return Optional.ofNullable(((HasStatus) this).save());
        else
            return Optional.empty();
    }
    
}
