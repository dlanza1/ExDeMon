package ch.cern.exdemon.components;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Optional;

import org.junit.Test;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentRegistrationResult.Status;

public class ComponentRegistrationResultTest {
    
    @Test
    public void okStatus() {
        ConfigurationResult configurationResult = ConfigurationResult.SUCCESSFUL();
        
        ComponentRegistrationResult regResult = ComponentRegistrationResult.from(
                                                            ComponentBuildResult.from(
                                                                    Type.MONITOR, 
                                                                    Optional.of("test-id"), 
                                                                    Optional.empty(), 
                                                                    configurationResult));
        
        assertEquals(Status.OK, regResult.getStatus());
    }
    
    @Test
    public void warningStatus() {
        ConfigurationResult configurationResult = ConfigurationResult.SUCCESSFUL();
        configurationResult.withWarning("pw1", "w1");
        configurationResult.withWarning("pw2", "w2");
        
        ComponentRegistrationResult regResult = ComponentRegistrationResult.from(
                                                            ComponentBuildResult.from(
                                                                    Type.MONITOR, 
                                                                    Optional.of("test-id"),
                                                                    Optional.empty(), 
                                                                    configurationResult));
        
        assertEquals(Status.WARNING, regResult.getStatus());
    }
    
    @Test
    public void errorStatus() {
        ConfigurationResult configurationResult = ConfigurationResult.SUCCESSFUL();
        configurationResult.withError("e1", "em1");
        
        ComponentRegistrationResult regResult = ComponentRegistrationResult.from(
                                                            ComponentBuildResult.from(
                                                                    Type.MONITOR, 
                                                                    Optional.of("test-id"),
                                                                    Optional.empty(), 
                                                                    configurationResult));
        
        assertEquals(Status.ERROR, regResult.getStatus());
    }

    @Test
    public void toJsonWithErrorsAndWarnings() {
        ConfigurationResult configurationResult = ConfigurationResult.SUCCESSFUL();
        configurationResult.withError("pe1", new ParseException("m1", 0));
        configurationResult.withError("pe2", "e2");
        configurationResult.withWarning("pw1", "w1");
        configurationResult.withWarning("pw2", "w2");
        
        ComponentRegistrationResult regResult = ComponentRegistrationResult.from(ComponentBuildResult.from(Type.MONITOR, Optional.of("test-id"), Optional.empty(), configurationResult));
        
        String jsonString = regResult.toJsonString();
        
        assertTrue(jsonString.contains("ERROR"));
        assertTrue(jsonString.contains("pe1"));
        assertTrue(jsonString.contains("pw2"));
    }
    
}
