package ch.cern.components;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import ch.cern.components.Component.Type;
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
        Component component = ComponentsCatalog.register(Type.MONITOR, "id", properties);

        Optional<Component> secondComponentOpt = ComponentsCatalog.get(Type.MONITOR, "id");
        
        assertTrue(secondComponentOpt.isPresent());
        assertSame(component, secondComponentOpt.get());
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
        Component component = ComponentsCatalog.register(Type.MONITOR, "id", properties);
        
        Component secondRegistration = ComponentsCatalog.register(Type.MONITOR, "id", properties);
        
        assertSame(component, secondRegistration);
    }
    
}
