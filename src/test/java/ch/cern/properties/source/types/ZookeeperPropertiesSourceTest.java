package ch.cern.properties.source.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class ZookeeperPropertiesSourceTest {
    
    private TestingServer zkTestServer;
    private ZooKeeper zk;

    private ArrayList<ACL> acls = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    private CreateMode mode = CreateMode.PERSISTENT;
    
    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer(2181);
        
        zk = new ZooKeeper("localhost:2181", 1000, null);
        Thread.sleep(100);
        
        zk.create("/exdemon_json", null, acls, mode);
        zk.create("/exdemon", null, acls, mode);
        zk.create("/exdemon/owner=exdemon", null, acls, mode);
        zk.create("/exdemon/owner=exdemon/env=qa", null, acls, mode);
        zk.create("/exdemon/owner=exdemon/env=qa/id=spark_batch", null, acls, mode);
        zk.create("/exdemon/owner=exdemon/env=qa/id=spark_batch/type=schema", null, acls, mode);
        zk.create("/exdemon/owner=exdemon/env=qa/id=spark_batch/type=schema/attributes", null, acls, mode);
        zk.create("/exdemon/owner=exdemon/env=qa/id=spark_batch/type=schema/attributes/$environment", "qa".getBytes(), acls, mode);
        zk.create("/exdemon/owner=tape", null, acls, mode);
        zk.create("/exdemon/owner=tape/env=tapeserver_diskserver", null, acls, mode);
        zk.create("/exdemon/owner=tape/env=tapeserver_diskserver/id=perf", null, acls, mode);
        zk.create("/exdemon/owner=tape/env=tapeserver_diskserver/id=perf/type=schema", null, acls, mode);
        zk.create("/exdemon/owner=tape/env=tapeserver_diskserver/id=perf/type=schema/timestamp", null, acls, mode);
        zk.create("/exdemon/owner=tape/env=tapeserver_diskserver/id=perf/type=schema/timestamp/key", "data.timestamp".getBytes(), acls, mode);
        zk.create("/exdemon/owner=db", null, acls, mode);
        zk.create("/exdemon/owner=db/env=production", null, acls, mode);
        zk.create("/exdemon/owner=db/env=production/id=inventory-missing", null, acls, mode);
        zk.create("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor", null, acls, mode);
        zk.create("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/triggers", null, acls, mode);
        zk.create("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/triggers/mattermost", null, acls, mode);
        zk.create("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/triggers/mattermost/actuators", "a1 a2 a3".getBytes(), acls, mode);
    }
    
    @Test
    public void hostNotAccesible() {
        ZookeeperPropertiesSource source = new ZookeeperPropertiesSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "UNKNOWN:2181/exdemon");
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
        sourceProperties.setProperty("connection_string", "localhost:2181/exdemon");
        sourceProperties.setProperty("initialization_timeout_ms", "100");
        sourceProperties.setProperty("timeout_ms", "100");
        
        source.config(sourceProperties);
        
        assertNotNull(source.load());
        
        zkTestServer.stop();
        zkTestServer = null;
        
        Thread.sleep(1000);
        
        try {
            source.load();
        }catch(IOException e) {}
    }
    
    @Test
    public void pathDoesNotExist() {
        ZookeeperPropertiesSource source = new ZookeeperPropertiesSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2181/UNKNOWN");
        
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
        sourceProperties.setProperty("connection_string", "localhost:2181/exdemon");
        source.config(sourceProperties);
        
        Properties props = new Properties();
        props.setProperty("metrics.schema.exdemon_qa_spark_batch.attributes.$environment", "qa");
        props.setProperty("metrics.schema.tape_tapeserver_diskserver_perf.timestamp.key", "data.timestamp");
        props.setProperty("monitor.db_production_inventory-missing.triggers.mattermost.actuators", "a1 a2 a3");
        
        assertEquals(props, source.load());
    }
    
    @Test
    public void parsePropertiesAsJSON() throws Exception {
        ZookeeperPropertiesSource source = new ZookeeperPropertiesSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2181/exdemon");
        sourceProperties.setProperty("asjson", "json");
        source.config(sourceProperties);
        
        String json = "{ \"a\": 12, \"b\": { \"c\": 34 }}";
        
        zk.create("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/json", json.getBytes(), acls, mode);
        
        Properties props = new Properties();
        props.setProperty("monitor.db_production_inventory-missing.a", "12");
        props.setProperty("monitor.db_production_inventory-missing.b.c", "34");
        
        assertEquals(props, source.load());
    }
    
    @Test
    public void updatePropertiesAsJSON() throws Exception {
        ZookeeperPropertiesSource source = new ZookeeperPropertiesSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2181/exdemon");
        sourceProperties.setProperty("asjson", "json");
        source.config(sourceProperties);
        
        String json = "{ \"a\": 12, \"b\": { \"c\": 34 }}";
        
        zk.create("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/json", json.getBytes(), acls, mode);
        
        Properties props = new Properties();
        props.setProperty("monitor.db_production_inventory-missing.a", "12");
        props.setProperty("monitor.db_production_inventory-missing.b.c", "34");
        
        json = "{ \"z\": 52, \"b\": 98 }";
        zk.setData("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/json", json.getBytes(), -1);
        
        props = new Properties();
        props.setProperty("monitor.db_production_inventory-missing.b", "98");
        props.setProperty("monitor.db_production_inventory-missing.z", "52");
        assertEquals(props, source.load());
    }
    
    @Test
    public void removePropertiesAsJSON() throws Exception {
        ZookeeperPropertiesSource source = new ZookeeperPropertiesSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2181/exdemon");
        sourceProperties.setProperty("asjson", "json");
        source.config(sourceProperties);
        
        String json = "{ \"a\": 12, \"b\": { \"c\": 34 }}";
        
        zk.create("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/json", json.getBytes(), acls, mode);
        
        Properties props = new Properties();
        props.setProperty("monitor.db_production_inventory-missing.a", "12");
        props.setProperty("monitor.db_production_inventory-missing.b.c", "34");
        
        assertEquals(props, source.load());
        
        zk.delete("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/json", -1);
        
        assertEquals(0, source.load().size());
    }
    
    @Test
    public void removeProperties() throws Exception {
        ZookeeperPropertiesSource source = new ZookeeperPropertiesSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2181/exdemon");
        source.config(sourceProperties);
        
        Properties props = new Properties();
        props.setProperty("metrics.schema.exdemon_qa_spark_batch.attributes.$environment", "qa");
        props.setProperty("metrics.schema.tape_tapeserver_diskserver_perf.timestamp.key", "data.timestamp");
        props.setProperty("monitor.db_production_inventory-missing.triggers.mattermost.actuators", "a1 a2 a3");
        
        assertEquals(props, source.load());
        
        zk.delete("/exdemon/owner=exdemon/env=qa/id=spark_batch/type=schema/attributes/$environment", -1);
        zk.delete("/exdemon/owner=tape/env=tapeserver_diskserver/id=perf/type=schema/timestamp/key", -1);
        props.remove("metrics.schema.exdemon_qa_spark_batch.attributes.$environment");
        props.remove("metrics.schema.tape_tapeserver_diskserver_perf.timestamp.key");
        
        Thread.sleep(100);
        assertEquals(props, source.load());
        
        zk.delete("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/triggers/mattermost/actuators", -1);
        
        Thread.sleep(100);
        assertEquals(new Properties(), source.load());
    }
    
    @Test
    public void updateProperties() throws Exception {
        ZookeeperPropertiesSource source = new ZookeeperPropertiesSource();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("connection_string", "localhost:2181/exdemon");
        source.config(sourceProperties);
        
        Properties props = new Properties();
        props.setProperty("metrics.schema.exdemon_qa_spark_batch.attributes.$environment", "qa");
        props.setProperty("metrics.schema.tape_tapeserver_diskserver_perf.timestamp.key", "data.timestamp");
        props.setProperty("monitor.db_production_inventory-missing.triggers.mattermost.actuators", "a1 a2 a3");
        
        assertEquals(props, source.load());
        zk.setData("/exdemon/owner=exdemon/env=qa/id=spark_batch/type=schema/attributes/$environment", "qa_v2".getBytes(), -1);
        zk.setData("/exdemon/owner=tape/env=tapeserver_diskserver/id=perf/type=schema/timestamp/key", "data.timestamp_v2".getBytes(), -1);
        zk.setData("/exdemon/owner=db/env=production/id=inventory-missing/type=monitor/triggers/mattermost/actuators", "a1 a2 a3_v2".getBytes(), -1);
        props.setProperty("metrics.schema.exdemon_qa_spark_batch.attributes.$environment", "qa_v2");
        props.setProperty("metrics.schema.tape_tapeserver_diskserver_perf.timestamp.key", "data.timestamp_v2");
        props.setProperty("monitor.db_production_inventory-missing.triggers.mattermost.actuators", "a1 a2 a3_v2");
        
        Thread.sleep(100);
        assertEquals(props, source.load());
    }
    
    @Test
    public void toPropertyKey() {
        assertEquals("id.triggers.mattermost.actuators", ZookeeperPropertiesSource.toPropertyKey(null, null, "id", 
                     "/owner=db/env=production/id=inventory-missing/type=monitor/triggers/mattermost/actuators"));
        assertEquals("OW_UNKNOWN_id.triggers.mattermost.actuators", ZookeeperPropertiesSource.toPropertyKey("OW", null, "id", 
                     "/owner=db/env=production/id=inventory-missing/type=monitor/triggers/mattermost/actuators"));
        assertEquals("UNKNOWN_ENV_id.triggers.mattermost.actuators", ZookeeperPropertiesSource.toPropertyKey(null, "ENV", "id", 
                     "/owner=db/env=production/id=inventory-missing/type=monitor/triggers/mattermost/actuators"));
        assertEquals("OW_ENV_id.triggers.mattermost.actuators", ZookeeperPropertiesSource.toPropertyKey("OW", "ENV", "id", 
                     "/owner=db/env=production/id=inventory-missing/type=monitor/triggers/mattermost/actuators"));
    }
    
    @After
    public void shutDown() throws IOException, InterruptedException {
        if(zkTestServer != null)
            zkTestServer.close();
        zk.close();
    }
    
}
