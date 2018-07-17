package ch.cern.exdemon.components.source;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentRegistrationResult;
import ch.cern.exdemon.components.ComponentRegistrationResult.Status;
import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.components.source.types.TestComponentsSource;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class ComponentsSourceTest {
    
    @Before
    public void setUp() throws Exception {
        Properties componentsSourceProperties = new Properties();
        componentsSourceProperties.setProperty("type", "test");
        ComponentsCatalog.init(componentsSourceProperties);
        ComponentsCatalog.reset();
        ComponentsCatalog.resetSource();
    }
    
    @Test
    public void register() throws ConfigurationException {
        TestComponentsSource source = new TestComponentsSource();
        Properties sourceProps = new Properties();
        source.config(sourceProps);
        
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        source.register(Type.MONITOR, "id", properties);
        
        assertTrue(ComponentsCatalog.get(Type.MONITOR, "id").isPresent());
    }
    
    @Test
    public void remove() throws ConfigurationException {
        TestComponentsSource source = new TestComponentsSource();
        Properties sourceProps = new Properties();
        source.config(sourceProps);
        
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        source.register(Type.MONITOR, "id", properties);
        
        assertTrue(ComponentsCatalog.get(Type.MONITOR, "id").isPresent());
        
        source.remove(Type.MONITOR, "id");
        
        assertFalse(ComponentsCatalog.get(Type.MONITOR, "id").isPresent());
    }
    
    @Test
    public void okConfiguration() throws ConfigurationException {
        TestComponentsSource source = new TestComponentsSource();
        Properties sourceProps = new Properties();
        source.config(sourceProps);
        
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        Optional<ComponentRegistrationResult> componentRegistrationResult = source.register(Type.MONITOR, "id", properties);
        
        assertEquals(Status.OK, componentRegistrationResult.get().getStatus());
    }
    
    @Test
    public void errorConfiguration() throws ConfigurationException {
        TestComponentsSource source = new TestComponentsSource();
        Properties sourceProps = new Properties();
        source.config(sourceProps);
        
        Properties properties = new Properties();
        properties.setProperty("analysis.type", "no-exist");
        Optional<ComponentRegistrationResult> componentRegistrationResult = source.register(Type.MONITOR, "id", properties);
        
        assertEquals(Status.ERROR, componentRegistrationResult.get().getStatus());
    }
    
    @Test
    public void idFilter() throws ConfigurationException {
        TestComponentsSource source = new TestComponentsSource();
        Properties sourceProps = new Properties();
        sourceProps.setProperty("id.filters.qa", "qa_.*");
        source.config(sourceProps);
        
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        source.register(Type.MONITOR, "id", properties);
        
        properties = new Properties();
        properties.setProperty("filter.attribute.dummy", "dummy");
        source.register(Type.MONITOR, "qa_id", properties);
        
        assertFalse(ComponentsCatalog.get(Type.MONITOR, "id").isPresent());
        assertTrue(ComponentsCatalog.get(Type.MONITOR, "qa_id").isPresent());
    }

}
