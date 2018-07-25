package ch.cern.exdemon.components;

import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.properties.ConfigurationException;
import lombok.Getter;
import lombok.ToString;

@ToString
public class ComponentBuildResult<C extends Component> {
    
    @Getter
    private Type componentType;
    
    @Getter
    private String componentId;
    
    private C component;
    
    private ConfigurationException exception;
    
    @Getter
    private List<String> warnings = new LinkedList<>();
    
    private ComponentBuildResult() {
    }

    public static<C extends Component> ComponentBuildResult<C> from(Type componentType, C component) {
        ComponentBuildResult<C> result = new ComponentBuildResult<>();
        
        result.componentType = componentType;
        result.component = component;
        
        return result;
    }
    
    public static<C extends Component> ComponentBuildResult<C> fromException(Type componentType, String message) {
        return from(componentType, new ConfigurationException(message));
    }
    
    public static<C extends Component> ComponentBuildResult<C> from(Type componentType, ConfigurationException exception) {
        ComponentBuildResult<C> result = new ComponentBuildResult<>();
        
        result.componentType = componentType;
        result.exception = exception;
        
        return result;
    }
    
    public Optional<C> getComponent() {
        return Optional.ofNullable(component);
    }
    
    public Optional<ConfigurationException> getException() {
        return Optional.ofNullable(exception);
    }

    public void throwExceptionIfPresent() throws ConfigurationException {
        if(exception != null)
            throw exception;
    }
    
    public void setComponentId(String id){
        componentId = id;
        
        if(component != null)
            component.setId(id);
    }

}
