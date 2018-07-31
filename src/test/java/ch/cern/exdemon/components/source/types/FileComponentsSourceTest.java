package ch.cern.exdemon.components.source.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import ch.cern.exdemon.components.Component;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.properties.Properties;

public class FileComponentsSourceTest {
    
    @Before
    public void clear() {
        ComponentsCatalog.reset();
    }
    
    @Test
    public void register() throws Exception {
        FileComponentsSource source = spy(new FileComponentsSource());
        Properties sourceProps = new Properties();
        sourceProps.setProperty("path", "src/test/resources/config.properties");
        sourceProps.setProperty("expire", "1s");
        source.config(sourceProps);
        source.initialize();
        
        Properties properties = new Properties();
        properties.setProperty("monitors.m1.filter.attribute.dummy", "dummy");
        properties.setProperty("monitors.m2.filter.attribute.dummy", "dummy");
        when(source.loadProperties()).thenReturn(properties);
        
        Thread.sleep(1000);
        
        assertTrue(ComponentsCatalog.get(Type.MONITOR, "m1").isPresent());
        assertTrue(ComponentsCatalog.get(Type.MONITOR, "m2").isPresent());
        assertEquals(2, ComponentsCatalog.get(Type.MONITOR).size());
    }
    
    @Test
    public void update() throws Exception {
        FileComponentsSource source = spy(new FileComponentsSource());
        Properties sourceProps = new Properties();
        sourceProps.setProperty("path", "src/test/resources/config.properties");
        sourceProps.setProperty("expire", "1s");
        source.config(sourceProps);
        source.initialize();
        
        // Register one component
        Properties properties = new Properties();
        properties.setProperty("monitors.m1.filter.attribute.dummy", "dummy");
        int initPropertiesHash = properties.getSubset("monitors.m1").hashCode();
        when(source.loadProperties()).thenReturn(properties);
        Thread.sleep(1000);
        Optional<Component> initialComponent = ComponentsCatalog.get(Type.MONITOR, "m1");
        assertTrue(initialComponent.isPresent());
        assertEquals(initPropertiesHash, initialComponent.get().getPropertiesHash());
        assertEquals(1, ComponentsCatalog.get(Type.MONITOR).size());
        
        // Update previous component
        Properties updatedProperties = new Properties();
        updatedProperties.setProperty("monitors.m1.filter.attribute.dummy2", "dummy2");
        int updatedPropertiesHash = updatedProperties.getSubset("monitors.m1").hashCode();
        when(source.loadProperties()).thenReturn(updatedProperties);
        Thread.sleep(1000);
        Optional<Component> updatedComponent = ComponentsCatalog.get(Type.MONITOR, "m1");
        assertTrue(updatedComponent.isPresent());
        assertEquals(updatedPropertiesHash, updatedComponent.get().getPropertiesHash());
        assertEquals(1, ComponentsCatalog.get(Type.MONITOR).size());
        
        // Ensure different props
        assertNotEquals(initialComponent.get().getPropertiesHash(), 
                        updatedComponent.get().getPropertiesHash());
    }
    
    @Test
    public void delete() throws Exception {
        FileComponentsSource source = spy(new FileComponentsSource());
        Properties sourceProps = new Properties();
        sourceProps.setProperty("path", "src/test/resources/config.properties");
        sourceProps.setProperty("expire", "1s");
        source.config(sourceProps);
        source.initialize();
        
        // Register two components
        Properties properties = new Properties();
        properties.setProperty("monitors.m1.filter.attribute.dummy", "dummy");
        properties.setProperty("monitors.m2.filter.attribute.dummy", "dummy");
        when(source.loadProperties()).thenReturn(properties);
        Thread.sleep(1100);
        assertTrue(ComponentsCatalog.get(Type.MONITOR, "m1").isPresent());
        assertTrue(ComponentsCatalog.get(Type.MONITOR, "m2").isPresent());
        assertEquals(2, ComponentsCatalog.get(Type.MONITOR).size());
        
        // Remove one component
        Properties uodatedProperties = new Properties();
        uodatedProperties.setProperty("monitors.m1.filter.attribute.dummy", "dummy");
        when(source.loadProperties()).thenReturn(uodatedProperties);
        Thread.sleep(1100);
        assertTrue(ComponentsCatalog.get(Type.MONITOR, "m1").isPresent());
        assertFalse(ComponentsCatalog.get(Type.MONITOR, "m2").isPresent());
        assertEquals(1, ComponentsCatalog.get(Type.MONITOR).size());
    }

}
