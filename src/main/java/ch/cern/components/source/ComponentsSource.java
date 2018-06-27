package ch.cern.components.source;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.log4j.Logger;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.components.ComponentTypes;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

@ComponentType(Type.COMPONENTS_SOURCE)
public abstract class ComponentsSource extends Component {

    private static final long serialVersionUID = -7276957377600776486L;
    
    private final static Logger LOG = Logger.getLogger(ComponentsSource.class.getName());
    
    private Map<Component.Type, Map<String, Component>> components = new HashMap<>();
    
    public ComponentsSource() {
        components.put(Type.SCHEMA, new HashMap<>());
        components.put(Type.METRIC, new HashMap<>());
        components.put(Type.MONITOR, new HashMap<>());
        components.put(Type.ACTUATOR, new HashMap<>());
    }
    
    protected void register(Type componentType, String id, Properties properties) {
        Map<String, Component> componentsOfType = components.get(componentType);
        
        if(componentsOfType == null)
            throw new RuntimeException(componentType + " type cannot be in the catalog"); 
        
        try {
            Component component = ComponentTypes.build(componentType, id, properties);
            
            components.get(componentType).put(id, component);
            
            registerConfigurationOK(componentType, id, component);
        } catch (ConfigurationException e) {
            registerConfigurationError(componentType, id, e);
        }
    }
    
    private void registerConfigurationOK(Type componentType, String id, Component component) {
        LOG.info("Configuration OK for component of type="+componentType+" id="+id+" component="+component);
    }

    protected void registerConfigurationError(Type componentType, String id, ConfigurationException e) {
        LOG.error("Error for component of type="+componentType+" id="+id+" message="+e.getMessage(), e);
    }

    protected void remove(Type componentType, String id) {
        Map<String, Component> componentsOfType = components.get(componentType);
        
        if(componentsOfType == null)
            throw new RuntimeException(componentType + " type cannot be in the catalog");
        
        componentsOfType.remove(id);
    }
    
    @SuppressWarnings("unchecked")
    public <C extends Component> Optional<C> get(Type componentType, String id) {
        Map<String, Component> componentsOfType = components.get(componentType);
        
        if(componentsOfType == null)
            throw new RuntimeException(componentType + " type cannot be in the catalog");
        
        return Optional.ofNullable((C) componentsOfType.get(id));
    }

    public void initialize() throws Exception {
    }

    public void close() {
    }

}
