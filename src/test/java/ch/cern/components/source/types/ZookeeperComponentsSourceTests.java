package ch.cern.components.source.types;

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

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentsCatalog;
import ch.cern.properties.Properties;

public class ZookeeperComponentsSourceTests {

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
        ComponentsCatalog.init(sourceProperties );
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
        
        Thread.sleep(100);
        
        Optional<Component> componentOpt = ComponentsCatalog.get(Type.MONITOR, "id_test");
        
        assertTrue(componentOpt.isPresent());
    }
    
    @After
    public void shutDown() throws IOException, InterruptedException {
        if(zkTestServer != null)
            zkTestServer.close();
        client.close();
    }
    
}
