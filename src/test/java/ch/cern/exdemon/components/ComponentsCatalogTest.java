package ch.cern.exdemon.components;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentRegistrationResult.Status;
import ch.cern.properties.Properties;

public class ComponentsCatalogTest {
    
    @Before
    public void setUp() throws Exception {
        Properties componentsSourceProperties = new Properties();
        componentsSourceProperties.setProperty("type", "test");
        ComponentsCatalog.init(componentsSourceProperties);
    }
    
    @Test
    public void register() throws Exception {        
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        ComponentRegistrationResult componentRegistrationResult = ComponentsCatalog.register(Type.MONITOR, "id", properties);

        Optional<Component> secondComponentOpt = ComponentsCatalog.get(Type.MONITOR, "id");
        
        assertTrue(secondComponentOpt.isPresent());
        assertSame(componentRegistrationResult.getComponent().get(), secondComponentOpt.get());
    }
    
    @Test
    public void remove() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        ComponentsCatalog.register(Type.MONITOR, "id", properties);

        ComponentsCatalog.remove(Type.MONITOR, "id");
        assertFalse(ComponentsCatalog.get(Type.MONITOR, "id").isPresent());
    }
    
    @Test
    public void removeIfConfigurationError() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        ComponentsCatalog.register(Type.MONITOR, "id", properties);
        assertTrue(ComponentsCatalog.get(Type.MONITOR, "id").isPresent());
        
        Properties wrongProperties = new Properties();
        wrongProperties.setProperty("filter.expr", "dummy");
        
        ComponentRegistrationResult componentRegistrationResult = ComponentsCatalog.register(Type.MONITOR, "id", wrongProperties);

        assertEquals(Status.ERROR, componentRegistrationResult.getStatus());
        assertFalse(ComponentsCatalog.get(Type.MONITOR, "id").isPresent());
    }
    
    @Test
    public void reset() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        ComponentsCatalog.register(Type.MONITOR, "id", properties);

        ComponentsCatalog.reset();
        assertFalse(ComponentsCatalog.get(Type.MONITOR, "id").isPresent());
    }
    
    @Test
    public void sameComponentIfSameProperties() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        ComponentRegistrationResult componentRegistrationResult = ComponentsCatalog.register(Type.MONITOR, "id", properties);
        
        ComponentRegistrationResult componentRegistrationResult2 = ComponentsCatalog.register(Type.MONITOR, "id", properties);
        
        assertEquals(Status.EXISTING, componentRegistrationResult2.getStatus());
        assertSame(componentRegistrationResult.getComponent().get(), componentRegistrationResult2.getComponent().get());
    }
    
}
