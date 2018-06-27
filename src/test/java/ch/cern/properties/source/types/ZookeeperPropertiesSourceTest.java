package ch.cern.properties.source.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class ZookeeperPropertiesSourceTest {
    
    private TestingServer zkTestServer;
    private ZooKeeper zk;
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

        zk = client.getZookeeperClient().getZooKeeper();
    }
    
    @Test
    public void hostNotAccesible() {
        ZookeeperPropertiesSource source = new ZookeeperPropertiesSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "UNKNOWN:2182/exdemon");
        sourceProperties.setProperty("initialization_timeout_ms", "500");

        try{
            source.config(sourceProperties);
        }catch(ConfigurationException e) {
            assertTrue(e.getMessage().startsWith("java.io.IOException: Initialization timed-out"));
        }
    }
    
    @Test
    public void hostNotAccesibleAfterInitialization() throws Exception {
        ZookeeperPropertiesSource source = new ZookeeperPropertiesSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2182/exdemon");
        sourceProperties.setProperty("initialization_timeout_ms", "100");
        sourceProperties.setProperty("timeout_ms", "100");
        
        source.config(sourceProperties);
        
        client.create().creatingParentsIfNeeded().forPath("/exdemon");
        
        assertNotNull(source.loadAll());
        
        zkTestServer.stop();
        zkTestServer = null;
        
        Thread.sleep(1000);
        
        try {
            source.loadAll();
        }catch(IOException e) {}
    }
    
    @Test
    public void pathDoesNotExist() {
        ZookeeperPropertiesSource source = new ZookeeperPropertiesSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2182/UNKNOWN");
        
        try{
            source.config(sourceProperties);
        }catch(ConfigurationException e) {
            assertEquals("java.io.IOException: Initialization error, parent path may not exist", e.getMessage());
        }
    }
    
    @Test
    public void parseProperties() throws Exception {
        ZookeeperPropertiesSource source = new ZookeeperPropertiesSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2182/exdemon");
        source.config(sourceProperties);
        
        String json = "{ \"a\": 12, \"b\": { \"c\": 34 }}";
        
        client.create().creatingParentsIfNeeded()
            .forPath("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/config", json.getBytes());
        
        Properties props = new Properties();
        props.setProperty("monitor.db_production_inventory-missing.a", "12");
        props.setProperty("monitor.db_production_inventory-missing.b.c", "34");
        
        assertEquals(props, source.loadAll());
    }
    
    @Test
    public void diffConfNodeName() throws Exception {
        ZookeeperPropertiesSource source = new ZookeeperPropertiesSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2182/exdemon");
        sourceProperties.setProperty("conf_node_name", "configuration_node");
        source.config(sourceProperties);
        
        String json = "{ \"a\": 12 }";
        
        client.create().creatingParentsIfNeeded()
            .forPath("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/configuration_node", json.getBytes());
        
        Properties props = new Properties();
        props.setProperty("monitor.db_production_inventory-missing.a", "12");
        
        assertEquals(props, source.loadAll());
    }
    
    @Test
    public void updateProperties() throws Exception {
        ZookeeperPropertiesSource source = new ZookeeperPropertiesSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2182/exdemon");
        source.config(sourceProperties);
        
        String json = "{ \"a\": 12, \"b\": { \"c\": 34 }}";
        
        client.create().creatingParentsIfNeeded()
            .forPath("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/config", json.getBytes());
        
        Properties props = new Properties();
        props.setProperty("monitor.db_production_inventory-missing.a", "12");
        props.setProperty("monitor.db_production_inventory-missing.b.c", "34");
        
        json = "{ \"z\": 52, \"b\": 98 }";
        zk.setData("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/config", json.getBytes(), -1);
        
        props = new Properties();
        props.setProperty("monitor.db_production_inventory-missing.b", "98");
        props.setProperty("monitor.db_production_inventory-missing.z", "52");
        assertEquals(props, source.loadAll());
    }
    
    @Test
    public void removeProperties() throws Exception {
        ZookeeperPropertiesSource source = new ZookeeperPropertiesSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2182/exdemon");
        source.config(sourceProperties);
        
        String json = "{ \"a\": 12, \"b\": { \"c\": 34 }}";
        
        client.create().creatingParentsIfNeeded()
            .forPath("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/config", json.getBytes());
        
        Properties props = new Properties();
        props.setProperty("monitor.db_production_inventory-missing.a", "12");
        props.setProperty("monitor.db_production_inventory-missing.b.c", "34");
        
        assertEquals(props, source.loadAll());
        
        zk.delete("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/config", -1);
        
        Thread.sleep(100);
        
        assertEquals(0, source.loadAll().size());
    }
    
    @After
    public void shutDown() throws IOException, InterruptedException {
        if(zkTestServer != null)
            zkTestServer.close();
        zk.close();
    }
    
}
