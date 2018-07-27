package ch.cern.exdemon.components;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class ConfigurationResultTest {

    @Test
    public void merge() {
        ConfigurationResult configResult1 = ConfigurationResult.SUCCESSFUL();
        
        ConfigurationResult configResult2 = ConfigurationResult.SUCCESSFUL();
        configResult2.withError("e", "message_e");
        configResult2.withWarning("w", "message_w");
        
        configResult1.merge("a", configResult2);
        
        assertEquals("message_e", configResult1.getErrors().get(0).getMessage());
        assertEquals("message_w", configResult1.getWarnings().get(0).getMessage());
    }
    
}
