package ch.cern.exdemon.components;

import java.lang.reflect.Type;
import java.util.LinkedList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

import ch.cern.properties.ConfigurationException;
import lombok.Getter;
import lombok.ToString;

@ToString
public class ConfigurationResult {
    
    public static final String MUST_BE_CONFIGURED_MSG = "must be configured";

    @Getter
    private List<ConfigurationException> errors = new LinkedList<>();
    
    @Getter
    private List<ConfigurationException> warnings = new LinkedList<>();

    private ConfigurationResult() {
    }
    
    public static ConfigurationResult SUCCESSFUL() {
        return new ConfigurationResult();
    }

    public ConfigurationResult withError(String parameter, String exceptionMessage) {
        errors.add(new ConfigurationException(parameter, exceptionMessage));
        
        return this;
    }
    
    public ConfigurationResult withError(String parameter, ConfigurationException exception) {
        if(parameter != null)
            if(exception.getParameter() == null)
                exception.setParameter(parameter);
            else
                exception.setParameter(parameter + "." + exception.getParameter());
        
        errors.add(exception);
        
        return this;
    }
    
    public ConfigurationResult withError(String parameter, Exception exception) {
        if(exception instanceof ConfigurationException)
            return withError(parameter, (ConfigurationException) exception);
        
        this.errors.add(new ConfigurationException(parameter, exception));
        
        return this;
    }


    public static ConfigurationResult fromError(String parameter, String message) {
        ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
        
        return confResult.withError(parameter, message);
    }
    
    public ConfigurationResult withWarning(String parameter, String message) {
        warnings.add(new ConfigurationException(parameter, message));
        
        return this;
    }

    public ConfigurationResult merge(String parameter, ConfigurationResult other) {
        if(parameter != null) {
            other.errors.stream().forEach(e -> {
                if(e.getParameter() == null)
                    e.setParameter(parameter);
                else
                    e.setParameter(parameter+"."+e.getParameter());
            });
            other.warnings.stream().forEach(e -> {
                if(e.getParameter() == null)
                    e.setParameter(parameter);
                else
                    e.setParameter(parameter+"."+e.getParameter());
            });
        }
        
        errors.addAll(other.errors);
        warnings.addAll(other.warnings);
        
        return this;
    }

    public ConfigurationResult withMustBeConfigured(String parameter) {
        return withError(parameter, MUST_BE_CONFIGURED_MSG);
    }
    
    public static class ConfigurationResultJsonSerializer implements JsonSerializer<ConfigurationResult> {

        @Override
        public JsonElement serialize(ConfigurationResult confgiResult, Type type, JsonSerializationContext context) {
            JsonObject object = new JsonObject();
            
            JsonArray errors = new JsonArray();
            for (ConfigurationException excep : confgiResult.getErrors()) {
                JsonObject excepJson = new JsonObject();
                excepJson.addProperty("parameter", excep.getParameter());
                excepJson.addProperty("message", excep.getMessage());
                errors.add(excepJson);
            }
            object.add("errors", errors);
            
            JsonArray warninigs = new JsonArray();
            for (ConfigurationException warn : confgiResult.getWarnings()) {
                JsonObject excepJson = new JsonObject();
                excepJson.addProperty("parameter", warn.getParameter());
                excepJson.addProperty("message", warn.getMessage());
                warninigs.add(excepJson);
            }
            object.add("warnings", warninigs);
            
            return object;
        }
        
    }
    
}
