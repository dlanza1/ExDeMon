package ch.cern.spark.status.storage.manager;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusOperation;
import ch.cern.spark.status.StatusValue;

public class ZookeeperStatusesOperationsReceiverTest {

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
        
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/qa", null);
        
        zk = client.getZookeeperClient().getZooKeeper();
    }
    
    @Test
    public void newOperation() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("connection_string", "localhost:2182/exdemon/operations/qa");
        ZookeeperStatusesOperationsReceiver_ receiver = new ZookeeperStatusesOperationsReceiver_(properties);
        receiver.onStart();
        
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/qa/id=1234/keys", "REMOVE".getBytes());
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/qa/id=1234/op", "REMOVE".getBytes());
        
        Thread.sleep(100);
        
        List<StatusOperation<StatusKey, StatusValue>> ops = receiver.getStoredOps();
        System.out.println(ops);
        
        System.out.println(new String(client.getData().forPath("/exdemon/operations/qa/id=1234/status")));
        
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
        
        private List<StatusOperation<StatusKey, StatusValue>> storedOps = new LinkedList<>();
        
        public ZookeeperStatusesOperationsReceiver_(Properties properties) {
            super(properties);
        }
        
        @Override
        public void store(StatusOperation<StatusKey, StatusValue> dataItem) {
            storedOps.add(dataItem);
        }
        
        public List<StatusOperation<StatusKey, StatusValue>> getStoredOps() {
            return storedOps;
        }
        
    }
    
}
