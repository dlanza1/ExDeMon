package ch.cern.exdemon.components;

import java.util.Optional;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.properties.ConfigurationException;
import lombok.Getter;
import lombok.ToString;

@ToString
public class ComponentRegistrationResult {
    
    @Getter
    private Type componentType;
    
    @Getter
    private String componentId;
    
    public enum Status{
        OK,
        WARNING,
        ERROR,
        EXISTING
    };
    @Getter
    private Status status;
    
    private transient Component component;

    public static ComponentRegistrationResult from(Component existingComponent) {
        return from(existingComponent, Status.OK);
    }
    
    public static ComponentRegistrationResult from(Component component, Status status) {
        ComponentRegistrationResult componentRegistration = new ComponentRegistrationResult();
        
        componentRegistration.status = status;
        
        if(!status.equals(Status.ERROR)) {
            componentRegistration.componentType = ComponentTypes.getType(component.getClass());
            componentRegistration.componentId = component.getId();
            componentRegistration.component = component;
        }
        
        return componentRegistration;
    }
    
    public Optional<Component> getComponent() {
        return Optional.ofNullable(component);
    }

    public static ComponentRegistrationResult from(Type componentType, String id, ConfigurationException exception) {
        ComponentRegistrationResult componentRegistration = new ComponentRegistrationResult();
        
        componentRegistration.componentType = componentType;
        componentRegistration.componentId = id;
        componentRegistration.component = null;
        componentRegistration.status = Status.ERROR;
        
        return componentRegistration;
    }
    
}
