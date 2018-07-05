package ch.cern.exdemon.components.source.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.junit.Before;
import org.junit.Test;

import ch.cern.exdemon.components.Component;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.properties.Properties;

public class ZookeeperComponentsSourceTest {

    @Before
    public void startZookeeper() throws Exception {
        ComponentsCatalog.resetSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("type", "test");
        ComponentsCatalog.init(sourceProperties);
        ComponentsCatalog.reset();
    }
    
    @Test
    public void register() throws Exception {
        ZookeeperComponentsSource source = new ZookeeperComponentsSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2182/exdemon");
        source.config(sourceProperties);
        
        String path = "/exdemon/id=id_test/type=monitor/config";
        String json = "{ \"filter.attribute.dummy\": \"dummy\"}";
        source.applyEvent(null, zookeeperEvent(TreeCacheEvent.Type.NODE_ADDED, path, json));
        
        Optional<Component> componentOpt = ComponentsCatalog.get(Type.MONITOR, "id_test");
        
        assertTrue(componentOpt.isPresent());
    }
    
    @Test
    public void update() throws Exception {
        ZookeeperComponentsSource source = new ZookeeperComponentsSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2182/exdemon");
        source.config(sourceProperties);
        
        String path = "/exdemon/id=id_test/type=monitor/config";
        
        //Create
        String json = "{ \"filter.attribute.dummy\": \"dummy\"}";
        int propertiesHash = Properties.fromJson(json).hashCode();
        source.applyEvent(null, zookeeperEvent(TreeCacheEvent.Type.NODE_ADDED, path, json));
        Optional<Component> componentOpt = ComponentsCatalog.get(Type.MONITOR, "id_test");
        assertTrue(componentOpt.isPresent());
        assertEquals(propertiesHash, componentOpt.get().getPropertiesHash());
        
        //Update
        String updatingJson = "{ \"filter.attribute.dummy2\": \"dummy2\"}";
        int updatingPropertiesHash = Properties.fromJson(updatingJson).hashCode();
        source.applyEvent(null, zookeeperEvent(TreeCacheEvent.Type.NODE_UPDATED, path, updatingJson));
        Optional<Component> componentUpdatedOpt = ComponentsCatalog.get(Type.MONITOR, "id_test");
        assertTrue(componentUpdatedOpt.isPresent());
        assertEquals(updatingPropertiesHash, componentUpdatedOpt.get().getPropertiesHash());
        
        //Ensure different
        assertNotEquals(componentOpt.get(), componentUpdatedOpt.get());
        assertNotEquals(propertiesHash, updatingPropertiesHash);
    }
    
    @Test
    public void remove() throws Exception {
        ZookeeperComponentsSource source = new ZookeeperComponentsSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2182/exdemon");
        source.config(sourceProperties);
        
        String path = "/exdemon/id=id_test/type=monitor/config";
        
        //Create
        String json = "{ \"filter.attribute.dummy\": \"dummy\"}";
        source.applyEvent(null, zookeeperEvent(TreeCacheEvent.Type.NODE_ADDED, path, json));
        Optional<Component> componentOpt = ComponentsCatalog.get(Type.MONITOR, "id_test");
        assertTrue(componentOpt.isPresent());
        
        //Remove
        source.applyEvent(null, zookeeperEvent(TreeCacheEvent.Type.NODE_REMOVED, path, null));
        
        assertFalse(ComponentsCatalog.get(Type.MONITOR, "id_test").isPresent());
    }
    
    private TreeCacheEvent zookeeperEvent(TreeCacheEvent.Type eventType, String path, String value) {
        TreeCacheEvent eventMocked = mock(TreeCacheEvent.class);
        when(eventMocked.getType()).thenReturn(eventType);
        
        ChildData childData = mock(ChildData.class);
        when(childData.getPath()).thenReturn(path);
        
        if(value != null)
            when(childData.getData()).thenReturn(value.getBytes());
        
        when(eventMocked.getData()).thenReturn(childData);
        
        return eventMocked;
    }
    
}
