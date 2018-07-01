package ch.cern.components.source;

import org.apache.log4j.Logger;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.components.ComponentsCatalog;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

@ComponentType(Type.COMPONENTS_SOURCE)
public abstract class ComponentsSource extends Component {

    private static final long serialVersionUID = -7276957377600776486L;
    
    private final static Logger LOG = Logger.getLogger(ComponentsSource.class.getName());
    
    public ComponentsSource() {
    }
    
    public final void register(Type componentType, String id, Properties properties) {
        try {
            Component component = ComponentsCatalog.register(componentType, id, properties);
            
            registerConfigurationOK(componentType, id, component);
        } catch (ConfigurationException e) {
            registerConfigurationError(componentType, id, e);
        }
    }
    
    protected final void remove(Type componentType, String id) {
        ComponentsCatalog.remove(componentType, id);
    }
    
    protected void registerConfigurationOK(Type componentType, String id, Component component) {
        LOG.info("Configuration OK for component of type="+componentType+" id="+id+" component="+component);
    }

    protected void registerConfigurationError(Type componentType, String id, ConfigurationException e) {
        LOG.error("Error for component of type="+componentType+" id="+id+" message="+e.getMessage(), e);
    }

    public void initialize() throws Exception {
    }

    public void close() {
    }

}
