package ch.cern.spark.status;

import static ch.cern.spark.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.test.TestingServer;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.zookeeper.ZooKeeper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentsCatalog;
import ch.cern.properties.Properties;
import ch.cern.spark.Batches;
import ch.cern.spark.StreamTestHelper;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.DefinedMetricStatuskey;
import ch.cern.spark.metrics.defined.DefinedMetrics;
import ch.cern.spark.status.StatusOperation.Op;
import ch.cern.spark.status.storage.JSONStatusSerializer;
import ch.cern.spark.status.storage.manager.ToStringPatternStatusKeyFilter;
import ch.cern.spark.status.storage.manager.ZookeeperStatusesOperationsReceiver;
import scala.Tuple2;

public class StatusTest extends StreamTestHelper<Metric, Metric> {

	private static final long serialVersionUID = -7474201355112609336L;
	
    private TestingServer zkTestServer;
    private ZooKeeper zk;
    private CuratorFramework client;
    
    private static JSONStatusSerializer serializer = new JSONStatusSerializer();
    
    @Before
    public void startZookeeper() throws Exception {
        ComponentsCatalog.reset();
    	
        zkTestServer = new TestingServer(2182);
        
        client = CuratorFrameworkFactory.builder()
                .connectString(zkTestServer.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .sessionTimeoutMs(20000)
                .build();
        client.start();
        
        zk = client.getZookeeperClient().getZooKeeper();
        
        Map<String, String> extraSparkConf = new HashMap<>();
    	extraSparkConf.put(ZookeeperStatusesOperationsReceiver.PARAM + ".connection_string", zkTestServer.getConnectString());
		super.setUp(extraSparkConf);
    }
    
	@Test
	public void listOperations() throws Exception {
        addInput(0,    Metric(1, 10f, "HOSTNAME=host1234"));
        addInput(0,    Metric(1, 20f, "HOSTNAME=host4321"));
        addExpected(0, Metric(1, 20f, "HOSTNAME=host4321", "$defined_metric=dm1"));
        addExpected(0, Metric(1, 10f, "HOSTNAME=host1234", "$defined_metric=dm1"));
        
        client.create().creatingParentsIfNeeded().forPath("/id=1122/ops", "LIST".getBytes());
        client.create().creatingParentsIfNeeded().forPath("/id=1122/status", "RECEIVED".getBytes());
        
        Batches<StatusOperation<DefinedMetricStatuskey, Metric>> opBatches = new Batches<>();
        List<Function<Tuple2<StatusKey, StatusValue>, Boolean>> filters = new LinkedList<>();
        filters.add(new ToStringPatternStatusKeyFilter(".*host4321.*"));
		opBatches.add(0, new StatusOperation<DefinedMetricStatuskey, Metric>("1122", filters));
		
		Properties properties = new Properties();
        properties.setProperty("variables.a.aggregate.type", "sum");
        properties.setProperty("variables.a.aggregate.attributes", "ALL");
        properties.setProperty("metrics.groupby", "HOSTNAME");
        properties.setProperty("when", "batch");
        ComponentsCatalog.register(Type.METRIC, "dm1", properties);
	        
        JavaDStream<Metric> metricsStream = createStream(Metric.class);
		JavaDStream<StatusOperation<DefinedMetricStatuskey, Metric>> operations = createStream(StatusOperation.class, opBatches);
        
		JavaDStream<Metric> results = DefinedMetrics.generate(metricsStream, null, Optional.of(operations));
        
        assertExpected(results);
        
        assertEquals("{\"id\":\"dm1\",\"metric_attributes\":{\"HOSTNAME\":\"host4321\"},\"fqcn-alias\":\"defined-metric-key\"}\n", 
        			 new String(client.getData().forPath("/id=1122/keys")));
        assertEquals("DONE", new String(client.getData().forPath("/id=1122/status")));
	}
	
    @Test
    public void showOperations() throws Exception {
        addInput(0,    Metric(1, 10f, "HOSTNAME=host1234"));
        addInput(0,    Metric(1, 20f, "HOSTNAME=host4321"));
        addExpected(0, Metric(1, 20f, "HOSTNAME=host4321", "$defined_metric=dm1"));
        addExpected(0, Metric(1, 10f, "HOSTNAME=host1234", "$defined_metric=dm1"));
        
        Batches<StatusOperation<DefinedMetricStatuskey, Metric>> opBatches = new Batches<>();
        Map<String, String> metric_attributes = new HashMap<>();
        metric_attributes.put("HOSTNAME", "host4321");
        DefinedMetricStatuskey key = new DefinedMetricStatuskey("dm1", metric_attributes );
        opBatches.add(0, new StatusOperation<DefinedMetricStatuskey, Metric>("1122", key, Op.SHOW));
        
        client.create().creatingParentsIfNeeded().forPath("/id=1122/ops", "SHOW".getBytes());
        client.create().creatingParentsIfNeeded().forPath("/id=1122/keys", serializer.fromKey(key));
        client.create().creatingParentsIfNeeded().forPath("/id=1122/status", "RECEIVED".getBytes());
        
        Properties properties = new Properties();
        properties.setProperty("variables.a.aggregate.type", "sum");
        properties.setProperty("variables.a.aggregate.attributes", "ALL");
        properties.setProperty("metrics.groupby", "HOSTNAME");
        properties.setProperty("when", "batch");
        ComponentsCatalog.register(Type.METRIC, "dm1", properties);
            
        JavaDStream<Metric> metricsStream = createStream(Metric.class);
        JavaDStream<StatusOperation<DefinedMetricStatuskey, Metric>> operations = createStream(StatusOperation.class, opBatches);
        
        JavaDStream<Metric> results = DefinedMetrics.generate(metricsStream, null, Optional.of(operations));
        
        assertExpected(results);
        
        assertTrue(new String(client.getData().forPath("/id=1122/values"))
                   .startsWith("{\"key\":" + new String(serializer.fromKey(key)) + ",\"value\":{"));
        assertEquals("DONE", new String(client.getData().forPath("/id=1122/status")));
    }
    
    @After
    public void shutDown() throws IOException, InterruptedException {
        if(zkTestServer != null)
            zkTestServer.close();
        zk.close();
    }
	
}
