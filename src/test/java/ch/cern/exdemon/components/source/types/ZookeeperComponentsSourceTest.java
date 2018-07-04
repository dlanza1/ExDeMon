package ch.cern.exdemon.components.source.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Optional;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.cern.exdemon.components.Component;
import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.source.types.ZookeeperComponentsSource;
import ch.cern.properties.Properties;

public class ZookeeperComponentsSourceTest {

    private TestingServer zkTestServer;
    private CuratorFramework client;
    
    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer(2182);
        
        client = CuratorFrameworkFactory.builder()
                .connectString(zkTestServer.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .sessionTimeoutMs(20000)
                .build();
        client.start();
        
        client.create().creatingParentsIfNeeded().forPath("/exdemon");
        
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
        source.initialize();
        
        String json = "{ \"filter.attribute.dummy\": \"dummy\"}";
        
        client.create().creatingParentsIfNeeded()
            .forPath("/exdemon/id=id_test/type=monitor/config", json.getBytes());
        
        Thread.sleep(500);
        
        Optional<Component> componentOpt = ComponentsCatalog.get(Type.MONITOR, "id_test");
        
        assertTrue(componentOpt.isPresent());
    }
    
    @Test
    public void update() throws Exception {
        ZookeeperComponentsSource source = new ZookeeperComponentsSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2182/exdemon");
        source.config(sourceProperties);
        source.initialize();
        
        //Create
        String json = "{ \"filter.attribute.dummy\": \"dummy\"}";
        int propertiesHash = Properties.fromJson(json).hashCode();
        client.create().creatingParentsIfNeeded().forPath("/exdemon/id=id_test/type=monitor/config", json.getBytes());
        Thread.sleep(500);
        Optional<Component> componentOpt = ComponentsCatalog.get(Type.MONITOR, "id_test");
        assertTrue(componentOpt.isPresent());
        assertEquals(propertiesHash, componentOpt.get().getPropertiesHash());
        
        //Update
        String updatingJson = "{ \"filter.attribute.dummy2\": \"dummy2\"}";
        int updatingPropertiesHash = Properties.fromJson(updatingJson).hashCode();
        client.setData().forPath("/exdemon/id=id_test/type=monitor/config", updatingJson.getBytes());
        Thread.sleep(500);
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
        source.initialize();
        
        //Create
        String json = "{ \"filter.attribute.dummy\": \"dummy\"}";
        client.create().creatingParentsIfNeeded().forPath("/exdemon/id=id_test/type=monitor/config", json.getBytes());
        Thread.sleep(500);
        Optional<Component> componentOpt = ComponentsCatalog.get(Type.MONITOR, "id_test");
        assertTrue(componentOpt.isPresent());
        
        //Remove
        client.delete().forPath("/exdemon/id=id_test/type=monitor/config");
        Thread.sleep(500);
        
        assertFalse(ComponentsCatalog.get(Type.MONITOR, "id_test").isPresent());
    }
    
    @After
    public void shutDown() throws IOException, InterruptedException {
        if(zkTestServer != null)
            zkTestServer.close();
        client.close();
    }
    
}
