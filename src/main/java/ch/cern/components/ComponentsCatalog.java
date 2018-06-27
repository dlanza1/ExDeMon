package ch.cern.components;

import java.util.Optional;

import org.apache.log4j.Logger;

import ch.cern.components.Component.Type;
import ch.cern.components.source.ComponentsSource;
import ch.cern.properties.Properties;

public class ComponentsCatalog {
    
    private transient final static Logger LOG = Logger.getLogger(ComponentsCatalog.class.getName());
    
    public static ComponentsSource source = null;
    
    public static void initialize(Properties properties) throws Exception {
        if(source == null)
            source = ComponentTypes.build(Type.COMPONENTS_SOURCE, properties);
        
        try {
            source.initialize();
        }catch(Exception e) {
            LOG.error("Error initializing", e);
            
            source.close();
            source = null;
            
            throw e;
        }
    }
    
    public static <C extends Component> Optional<C> get(Type componentType, String id) {
        if(source == null)
            throw new RuntimeException("ComponentsSource has failed or is not initialized");
        
        return source.get(componentType, id);
    }

}
