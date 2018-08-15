package ch.cern.exdemon.components;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentRegistrationResult.Status;
import ch.cern.exdemon.components.source.ComponentsSource;
import ch.cern.properties.Properties;

public class ComponentsCatalog {
    
    private transient final static Logger LOG = Logger.getLogger(ComponentsCatalog.class.getName());
    
    private static Map<Component.Type, Map<String, Component>> components = new ConcurrentHashMap<>();
    static {
        reset();
    }

    private static ComponentsSource source = null;
    
    public static void init(Properties properties) throws Exception {
        if(source != null)
            return;
        
        ComponentBuildResult<ComponentsSource> sourceBuildResult = ComponentTypes.build(Type.COMPONENTS_SOURCE, properties);
        sourceBuildResult.throwExceptionsIfPresent();
        
        source = sourceBuildResult.getComponent().get();
        
        reset();
        
        try {
            source.initialize();
            
            LOG.info("Source initialized");
        }catch(Exception e) {
            LOG.error("Error initializing", e);
            
            source.close();
            source = null;
            
            throw e;
        }
    }
    
    public static ComponentRegistrationResult register(Type componentType, String id, Properties properties) {
        Map<String, Component> componentsOfType = get(componentType);
        
        //Do not build and register component if it exists and has same configuration
        if(componentsOfType.containsKey(id)) {
            Component existingComponent = componentsOfType.get(id);
            int existingPropertiesHash = existingComponent.getPropertiesHash();
            
            int propertiesHash = properties.hashCode();
            
            if(propertiesHash == existingPropertiesHash)
                return ComponentRegistrationResult.from(existingComponent, Status.EXISTING);
        }
        
        ComponentBuildResult<Component> componentBuildResult = ComponentTypes.build(componentType, id, properties);
        
        componentBuildResult.getComponent().ifPresent(c -> {
            components.get(componentType).put(id, c);
        });
        
        if(!componentBuildResult.getConfigurationResult().getErrors().isEmpty())
            remove(componentType, id);
        
        return ComponentRegistrationResult.from(componentBuildResult);
    }
    
    @SuppressWarnings("unchecked")
    public static <C extends Component> Optional<C> get(Type componentType, String id) {
        return Optional.ofNullable((C) get(componentType).get(id));
    }

    @SuppressWarnings("unchecked")
    public static <C extends Component> Map<String, C> get(Type componentType) {
        Map<String, Component> componentsOfType = components.get(componentType);
        
        if(componentsOfType == null)
            throw new IllegalArgumentException(componentType + " type cannot be in the catalog");
        
        return componentsOfType.entrySet().stream()
                    .collect(Collectors.toMap(entry -> entry.getKey(), entry -> ((C) entry.getValue())));
    }
    
    public static void remove(Type componentType, String id) {
        if(!components.containsKey(componentType))
            throw new IllegalArgumentException(componentType + " type cannot be in the catalog");
        
        LOG.info("Component "+componentType+" with id="+id+" removed ");
        
        components.get(componentType).remove(id);
    }

    public static void reset() {
        components.put(Type.SCHEMA, new ConcurrentHashMap<>());
        components.put(Type.METRIC, new ConcurrentHashMap<>());
        components.put(Type.MONITOR, new ConcurrentHashMap<>());
        components.put(Type.ACTUATOR, new ConcurrentHashMap<>());
        components.put(Type.SILENCE, new ConcurrentHashMap<>());
    }

    public static void resetSource() {
        source = null;
    }

    public static void addToReport(Type componentType, String componentId, String reportName, String content) {
        if(source == null)
            return;
        
        source.addToReport(componentType, componentId, reportName, content);
    }

}
