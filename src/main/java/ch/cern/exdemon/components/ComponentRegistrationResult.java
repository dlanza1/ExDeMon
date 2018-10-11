package ch.cern.exdemon.components;

import java.time.Instant;
import java.util.Optional;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ConfigurationResult.ConfigurationResultJsonSerializer;
import ch.cern.exdemon.json.InstantJsonSerializer;
import ch.cern.properties.ConfigurationException;
import lombok.Getter;
import lombok.ToString;

@ToString
public class ComponentRegistrationResult {
    
    private final transient static Gson jsonParser = new GsonBuilder()
            .setPrettyPrinting()
            .registerTypeAdapter(Instant.class, new InstantJsonSerializer())
            .registerTypeAdapter(ConfigurationResult.class, new ConfigurationResultJsonSerializer())
            .create();
    
    private Instant timestamp;
    
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

    private ConfigurationResult configurationResult;
    
    private ComponentRegistrationResult() {
        this.timestamp = Instant.now();
    }
    
    public static ComponentRegistrationResult from(ComponentBuildResult<Component> componentBuildResult) {
        ComponentRegistrationResult componentRegistration = new ComponentRegistrationResult();
        
        componentRegistration.componentId = componentBuildResult.getComponentId();
        componentRegistration.componentType = componentBuildResult.getComponentType();
        componentRegistration.configurationResult = componentBuildResult.getConfigurationResult();
        
        componentBuildResult.getComponent().ifPresent(c -> {
            componentRegistration.component = c;
        });
        
        if(!componentBuildResult.getConfigurationResult().getErrors().isEmpty())
            componentRegistration.status = Status.ERROR;
        else if (!componentBuildResult.getConfigurationResult().getWarnings().isEmpty())
            componentRegistration.status = Status.WARNING;
        else
            componentRegistration.status = Status.OK;
        
        return componentRegistration;
    }

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

    public String toJsonString() {
        return jsonParser.toJson(this);
    }
    
}
