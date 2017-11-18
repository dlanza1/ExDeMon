package ch.cern.components;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.log4j.Logger;
import org.reflections.Reflections;

import ch.cern.components.Component.Type;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.store.Store;

public class ComponentManager {
    
    private final static Logger LOG = Logger.getLogger(ComponentManager.class.getName());
    
    private static Map<Component.Type, Map<String, Class<? extends Component>>> availableComponents = new HashMap<>();
    static{
        new Reflections("ch.cern")
	        		.getTypesAnnotatedWith(RegisterComponent.class)
	        		.stream()
	        		.forEach(ComponentManager::registerComponent);
    }
    
	@SuppressWarnings("unchecked")
	private static void registerComponent(Class<?> componentToRegister) {
    		Class<? extends Component> componentClass;
    		try {
    			 componentClass = (Class<? extends Component>) componentToRegister;
    		}catch(ClassCastException e){
    			LOG.error("Component " + componentToRegister + " could not be registered, it must extend " + Component.class);
    			return;
    		}
    		
    		ComponentType typeAnnotation = componentClass.getSuperclass().getAnnotation(ComponentType.class);
    		if(typeAnnotation == null)
    			typeAnnotation = componentClass.getSuperclass().getSuperclass().getAnnotation(ComponentType.class);
    		if(typeAnnotation == null) {
    			LOG.error("Component " + componentToRegister + " could not be registered, it does not extend a class with @ComponentType annotation");
    			return;
    		}
    			
        Type type = typeAnnotation.value();
        String name = componentClass.getAnnotation(RegisterComponent.class).value();
        
        if(!availableComponents.containsKey(type))
            availableComponents.put(type, new HashMap<String, Class<? extends Component>>());
        
        availableComponents.get(type).put(name, componentClass);
    }
    
    public static<C extends Component> C build(Type componentType, Properties properties) throws Exception {
        return build(componentType, null, properties);
    }
    
    public static<C extends Component> C build(Component.Type componentType, Optional<Store> store, Properties properties) throws Exception {
        String type = properties.getProperty("type");
        
        if(type == null)
            throw new ExceptionInInitializerError("Component type cannot be null.");
        
        C component = buildFromAvailableComponents(componentType, type);
        
        if(component == null){
            try {
                component = getComponentInstance(type);
            } catch (Exception e) {
                String message = "Component class could not be loaded, type or class (" + type + ") does not exist. ";
                
                if(getAvailableComponents(componentType) != null)
                		message += "It must be a FQCN or one of: " + getAvailableComponents(componentType).keySet();
                else
                		message += "It must be a FQCN (not built-in components availables)";
                
                LOG.error(message, e);
                throw new ExceptionInInitializerError(message);
            }
        }
        
        component.config(properties);
        
        if(component.hasStore())
            try{
            		store.ifPresent(((HasStore) component)::load);
            } catch(ClassCastException e){
                // In case Store has changed, we may get ClassCastException
                // We do nothing, proper Store will be set when saving
            }
        
        return component;
    }

    private static<C extends Component> C buildFromAvailableComponents(Type componentType, String type) throws Exception {
        Map<String, Class<? extends Component>> availableComponents = getAvailableComponents(componentType);
        
        if(availableComponents == null)
            return null;
        
        for (Map.Entry<String, Class<? extends Component>> availableComponent : availableComponents.entrySet())
            if(availableComponent.getKey().equals(type))
                return getComponentInstance(availableComponent.getValue().getName());
        
        return null;
    }

	private static<C extends Component> C getComponentInstance(String clazzName) throws Exception {
		@SuppressWarnings("unchecked")
		Class<C> clazz = (Class<C>) Class.forName(clazzName).asSubclass(Component.class);
		
		return clazz.newInstance();
    }

    public static Map<String, Class<? extends Component>> getAvailableComponents(Type componentType){
        return availableComponents.get(componentType);
    }

	public static<C extends Component> Optional<C> buildOptional(Type type, Optional<Store> store, Properties props) throws Exception {
		if(!props.isTypeDefined())
			return Optional.empty();
		
		return Optional.of(build(type, store, props));
	}

	public static <C extends Component> Optional<C> buildOptional(Type type, Properties props) throws Exception {
		if(!props.isTypeDefined())
			return Optional.empty();
		
		return Optional.of(build(type, props));
	}

}
