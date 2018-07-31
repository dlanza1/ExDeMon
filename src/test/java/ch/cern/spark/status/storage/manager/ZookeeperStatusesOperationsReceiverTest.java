package ch.cern.spark.status.storage.manager;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.spark.api.java.function.Function;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.cern.exdemon.monitor.MonitorStatusKey;
import ch.cern.exdemon.monitor.trigger.TriggerStatusKey;
import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusOperation;
import ch.cern.spark.status.StatusOperation.Op;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.JSONStatusSerializer;
import scala.Tuple2;

public class ZookeeperStatusesOperationsReceiverTest {
	
    private TestingServer zkTestServer;
    private ZooKeeper zk;
    private CuratorFramework client;
    private JSONStatusSerializer derializer = new JSONStatusSerializer();
    
    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer(2182);
        
        client = CuratorFrameworkFactory.builder()
                .connectString(zkTestServer.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .sessionTimeoutMs(20000)
                .build();
        client.start();
        
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/qa", null);
        
        zk = client.getZookeeperClient().getZooKeeper();
    }
    
    @Test
    public void removeOperationWithSingleKey() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("connection_string", "localhost:2182/exdemon/operations/qa");
        ZookeeperStatusesOperationsReceiver_ receiver = new ZookeeperStatusesOperationsReceiver_(properties);
        receiver.onStart();
        
        MonitorStatusKey key = new MonitorStatusKey("m1", new HashMap<>());
        
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/qa/id=1122/keys", derializer.fromKey(key));
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/qa/id=1122/ops", "REMOVE".getBytes());
        
        Thread.sleep(100);
        
        List<StatusOperation<StatusKey, ?>> ops = receiver.getStoredOps();
        
        assertEquals("RECEIVED", new String(client.getData().forPath("/exdemon/operations/qa/id=1122/status")));
        assertEquals(new StatusOperation<>("1122", key, Op.REMOVE), ops.get(0));
        
        receiver.onStop();
    }
    
    @Test
    public void removeOperationWithMultipleKeys() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("connection_string", "localhost:2182/exdemon/operations/qa");
        ZookeeperStatusesOperationsReceiver_ receiver = new ZookeeperStatusesOperationsReceiver_(properties);
        receiver.onStart();
        
        MonitorStatusKey key1 = new MonitorStatusKey("m1", new HashMap<>());
        String str1 = new String(derializer.fromKey(key1));
        TriggerStatusKey key2 = new TriggerStatusKey("m2", "tg1", new HashMap<>());
        String str2 = new String(derializer.fromKey(key2));
        
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/qa/id=1234/keys", (str1+"\n"+str2+"\n").getBytes());
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/qa/id=1234/ops", "REMOVE".getBytes());
        
        Thread.sleep(100);
        
        List<StatusOperation<StatusKey, ?>> ops = receiver.getStoredOps();
        
        assertEquals("RECEIVED", new String(client.getData().forPath("/exdemon/operations/qa/id=1234/status")));
        assertEquals(new StatusOperation<>("1234", key1, Op.REMOVE), ops.get(0));
        assertEquals(new StatusOperation<>("1234", key2, Op.REMOVE), ops.get(1));
        
        receiver.onStop();
    }
    
    @Test
    public void listOperationWithSingleFilter() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("connection_string", "localhost:2182/exdemon/operations/qa");
        ZookeeperStatusesOperationsReceiver_ receiver = new ZookeeperStatusesOperationsReceiver_(properties);
        receiver.onStart();
        
        String filtersString = "class abcd";
        
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/qa/id=1122/filters", filtersString.getBytes());
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/qa/id=1122/ops", "LIST".getBytes());
        
        Thread.sleep(100);
        
        List<StatusOperation<StatusKey, ?>> ops = receiver.getStoredOps();
        
        List<Function<Tuple2<StatusKey, StatusValue>, Boolean>> filters = new LinkedList<>();
        filters.add(new ClassNameStatusKeyFilter("abcd"));
        
        assertEquals("RECEIVED", new String(client.getData().forPath("/exdemon/operations/qa/id=1122/status")));
		assertEquals(new StatusOperation<>("1122", filters), ops.get(0));
        
        receiver.onStop();
    }
    
    @Test
    public void listOperationWithMultipleFilters() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("connection_string", "localhost:2182/exdemon/operations/qa");
        ZookeeperStatusesOperationsReceiver_ receiver = new ZookeeperStatusesOperationsReceiver_(properties);
        receiver.onStart();
        
        String filtersString = "class abcd\npattern .*tpsrv1234.*\n";
        
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/qa/id=1122/filters", filtersString.getBytes());
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/qa/id=1122/ops", "LIST".getBytes());
        
        Thread.sleep(100);
        
        List<StatusOperation<StatusKey, ?>> ops = receiver.getStoredOps();
        
        List<Function<Tuple2<StatusKey, StatusValue>, Boolean>> filters = new LinkedList<>();
        filters.add(new ClassNameStatusKeyFilter("abcd"));
        filters.add(new ToStringPatternStatusKeyFilter(".*tpsrv1234.*"));

        assertEquals("RECEIVED", new String(client.getData().forPath("/exdemon/operations/qa/id=1122/status")));
		assertEquals(new StatusOperation<>("1122", filters), ops.get(0));
        
        receiver.onStop();
    }
    
    @Test
    public void showOperation() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("connection_string", "localhost:2182/exdemon/operations/qa");
        ZookeeperStatusesOperationsReceiver_ receiver = new ZookeeperStatusesOperationsReceiver_(properties);
        receiver.onStart();
        
        MonitorStatusKey key1 = new MonitorStatusKey("m1", new HashMap<>());
        String str1 = new String(derializer.fromKey(key1));
        
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/qa/id=1122/keys", str1.getBytes());
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/qa/id=1122/ops", "SHOW".getBytes());
        
        Thread.sleep(100);
        
        List<StatusOperation<StatusKey, ?>> ops = receiver.getStoredOps();
        
        List<Function<Tuple2<StatusKey, StatusValue>, Boolean>> filters = new LinkedList<>();
        filters.add(new ClassNameStatusKeyFilter("abcd"));
        
        assertEquals("RECEIVED", new String(client.getData().forPath("/exdemon/operations/qa/id=1122/status")));
        assertEquals(new StatusOperation<>("1122", key1, Op.SHOW), ops.get(0));
        
        receiver.onStop();
    }
    
    @After
    public void shutDown() throws IOException, InterruptedException {
        if(zkTestServer != null)
            zkTestServer.close();
        zk.close();
    }
    
    public static class ZookeeperStatusesOperationsReceiver_ extends ZookeeperStatusesOperationsReceiver {

        private static final long serialVersionUID = 6275351290636755863L;
        
        private List<StatusOperation<StatusKey, ?>> storedOps = new LinkedList<>();
        
        public ZookeeperStatusesOperationsReceiver_(Properties properties) {
            super(properties);
        }
        
        @Override
        public void store(StatusOperation<StatusKey, ?> dataItem) {
            storedOps.add(dataItem);
        }
        
        public List<StatusOperation<StatusKey, ?>> getStoredOps() {
            return storedOps;
        }
        
    }
    
}
