package ch.cern.components.source;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.regex.Pattern;

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

    public static String PARAM = "components.source";
    
    private List<Pattern> id_filters = new LinkedList<>();

    private Properties staticProperties;
    
    public ComponentsSource() {
    }
    
    @Override
    protected final void config(Properties properties) throws ConfigurationException {
        Properties filtersProps = properties.getSubset("id.filters");
        if(filtersProps.size() > 0) {
            id_filters = new LinkedList<>();
            for (String id : filtersProps.getIDs())
                id_filters.add(Pattern.compile(filtersProps.getProperty(id)));
        }else{
            id_filters = Arrays.asList(Properties.ID_REGEX);
        }
        
        staticProperties = properties.getSubset("static");
        
        configure(properties);
    }
    
    protected void configure(Properties properties) throws ConfigurationException {
    }

    public final Optional<Component> register(Type componentType, String id, Properties properties) {
        try {
            boolean filter = filterID(id);
            if(!filter)
                return Optional.empty();
            
            properties.setStaticProperties(staticProperties);
            
            Component component = ComponentsCatalog.register(componentType, id, properties);
            
            registerConfigurationOK(componentType, id, component);
            
            return Optional.of(component);
        } catch (ConfigurationException e) {
            registerConfigurationError(componentType, id, e);
            
            return Optional.empty();
        }
    }
    
    private boolean filterID(String id) {
        for (Pattern id_filter : id_filters)
            if(id_filter.matcher(id).matches())
                return true;
        
        return false;
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
