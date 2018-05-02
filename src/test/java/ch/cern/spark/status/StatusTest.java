package ch.cern.spark.status;

import static ch.cern.spark.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;

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

import ch.cern.Cache;
import ch.cern.properties.Properties;
import ch.cern.spark.Batches;
import ch.cern.spark.StreamTestHelper;
import ch.cern.spark.metrics.Driver;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.DefinedMetricStatuskey;
import ch.cern.spark.metrics.defined.DefinedMetrics;
import ch.cern.spark.status.storage.manager.ToStringPatternStatusKeyFilter;
import scala.Tuple2;

public class StatusTest extends StreamTestHelper<Metric, Metric> {

	private static final long serialVersionUID = -7474201355112609336L;
	
    private TestingServer zkTestServer;
    private ZooKeeper zk;
    private CuratorFramework client;
    
    @Before
    public void startZookeeper() throws Exception {
    	Properties propertiesSourceProps = new Properties();
		propertiesSourceProps.setProperty("type", "static");
        Properties.initCache(propertiesSourceProps);
		DefinedMetrics.getCache().reset();
    	
        zkTestServer = new TestingServer(2182);
        
        client = CuratorFrameworkFactory.builder()
                .connectString(zkTestServer.getConnectString())
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .sessionTimeoutMs(20000)
                .build();
        client.start();
        
        client.create().creatingParentsIfNeeded().forPath("/id=1122/status", "OK".getBytes());
        
        zk = client.getZookeeperClient().getZooKeeper();
        
        Map<String, String> extraSparkConf = new HashMap<>();
    	extraSparkConf.put(Driver.STATUSES_OPERATIONS_RECEIVER_PARAM + ".connection_string", zkTestServer.getConnectString());
		super.setUp(extraSparkConf);
    }
    
	@Test
	public void shouldApplyListOperations() throws Exception {
		DefinedMetrics.getCache().reset();
		
        addInput(0,    Metric(1, 10f, "HOSTNAME=host1234"));
        addInput(0,    Metric(1, 20f, "HOSTNAME=host4321"));
        addExpected(0, Metric(1, 20f, "HOSTNAME=host4321", "$defined_metric=dm1"));
        addExpected(0, Metric(1, 10f, "HOSTNAME=host1234", "$defined_metric=dm1"));
        
        Batches<StatusOperation<DefinedMetricStatuskey, Metric>> opBatches = new Batches<>();
        List<Function<Tuple2<StatusKey, StatusValue>, Boolean>> filters = new LinkedList<>();
        filters.add(new ToStringPatternStatusKeyFilter(".*host4321.*"));
		opBatches.add(0, new StatusOperation<DefinedMetricStatuskey, Metric>("1122", filters));
		
        Cache<Properties> propertiesCache = Properties.getCache();
        propertiesCache.set(new Properties());
        propertiesCache.get().setProperty("metrics.define.dm1.variables.a.aggregate.type", "sum");
        propertiesCache.get().setProperty("metrics.define.dm1.variables.a.aggregate.attributes", "ALL");
        propertiesCache.get().setProperty("metrics.define.dm1.metrics.groupby", "HOSTNAME");
        propertiesCache.get().setProperty("metrics.define.dm1.when", "batch");
	        
        JavaDStream<Metric> metricsStream = createStream(Metric.class);
		JavaDStream<StatusOperation<DefinedMetricStatuskey, Metric>> operations = createStream(StatusOperation.class, opBatches);
        
		JavaDStream<Metric> results = DefinedMetrics.generate(metricsStream, null, Optional.of(operations));
        
        assertExpected(results);
        
        assertEquals("{\"id\":\"dm1\",\"metric_attributes\":{\"HOSTNAME\":\"host4321\"},\"fqcn-alias\":\"defined-metric-key\"}\n", 
        			 new String(client.getData().forPath("/id=1122/results")));
        assertEquals("FINISHED", new String(client.getData().forPath("/id=1122/status")));
	}
    
    @After
    public void shutDown() throws IOException, InterruptedException {
        if(zkTestServer != null)
            zkTestServer.close();
        zk.close();
    }
	
}
