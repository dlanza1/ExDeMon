package ch.cern.exdemon.components;

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
    
    @Getter
    private ConfigurationResult configurationResult;
    
    private ComponentBuildResult() {
    }
    
    public static<C extends Component> ComponentBuildResult<C> from(
            Type componentType, 
            Optional<String> idOpt,
            Optional<C> optional,
            ConfigurationResult configurationResult) {
        
        ComponentBuildResult<C> result = new ComponentBuildResult<>();
        
        idOpt.ifPresent(id -> result.componentId = id);
        
        result.componentType = componentType;
        result.configurationResult = configurationResult;
        
        optional.ifPresent(c -> result.component = c);
        
        return result;
    }
    
    public static<C extends Component> ComponentBuildResult<C> from(Type componentType, ConfigurationResult configurationResult) {
        ComponentBuildResult<C> result = new ComponentBuildResult<>();
        
        result.componentType = componentType;
        result.configurationResult = configurationResult;
        
        return result;
    }
    
    public Optional<C> getComponent() {
        return Optional.ofNullable(component);
    }
    
    public void setComponentId(String id){
        componentId = id;
        
        if(component != null)
            component.setId(id);
    }

    public void throwExceptionsIfPresent() throws ConfigurationException{
        if(configurationResult.getErrors().isEmpty())
            return;
        
        String message = configurationResult.getErrors().stream()
                                                .map(ConfigurationException::toString)
                                                .reduce("", (a,b) -> a.concat(b).concat(" - "));
        
        throw new ConfigurationException(null, message);
    }

}
