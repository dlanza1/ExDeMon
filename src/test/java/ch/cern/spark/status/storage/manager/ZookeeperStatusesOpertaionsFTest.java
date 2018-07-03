package ch.cern.spark.status.storage.manager;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.spark.api.java.function.Function;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.MonitorStatusKey;
import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusOperation;
import ch.cern.spark.status.StatusValue;
import scala.Tuple2;

public class ZookeeperStatusesOpertaionsFTest {
	
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
        
        client.create().creatingParentsIfNeeded().forPath("/exdemon/operations/env=qa/id=1122");
        
        zk = client.getZookeeperClient().getZooKeeper();
    }
    
    @Test
    public void filter() throws Exception {
    	Properties properties = new Properties();
        properties.setProperty("connection_string", "localhost:2182/exdemon/operations/env=qa");
    	ZookeeperStatusesOpertaionsF<MonitorStatusKey, Metric, StatusValue> f = new ZookeeperStatusesOpertaionsF<>(properties);
    	
    	List<Tuple2<Tuple2<MonitorStatusKey, StatusValue>, StatusOperation<MonitorStatusKey, Metric>>> tuples = new LinkedList<>();
    	List<Function<Tuple2<StatusKey, StatusValue>, Boolean>> filters = new LinkedList<>();
    	filters.add(new ToStringPatternStatusKeyFilter(".*tpsrv100.*"));
		StatusOperation<MonitorStatusKey, Metric> op = new StatusOperation<>("1122", filters);
		Map<String, String> atts1 = new HashMap<>();
		atts1.put("host", "tpsrv100");
		tuples.add(new Tuple2<>(new Tuple2<>(new MonitorStatusKey("m1", atts1), null), op));
		Map<String, String> atts2 = new HashMap<>();
		atts2.put("host", "tpsrv102");
		tuples.add(new Tuple2<>(new Tuple2<>(new MonitorStatusKey("m2", atts2), null), op));
    	
		f.call(tuples.iterator());
		
		assertEquals("{\"id\":\"m1\",\"metric_attributes\":{\"host\":\"tpsrv100\"},\"fqcn-alias\":\"monitor-key\"}\n", 
					 new String(client.getData().forPath("/exdemon/operations/env=qa/id=1122/keys")));
    }
    
    @After
    public void shutDown() throws IOException, InterruptedException {
        if(zkTestServer != null)
            zkTestServer.close();
        zk.close();
    }

}
