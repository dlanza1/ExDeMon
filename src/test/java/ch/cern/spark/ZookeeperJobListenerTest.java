package ch.cern.spark;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;

import org.apache.curator.test.TestingServer;
import org.apache.spark.SparkConf;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.scheduler.BatchInfo;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import ch.cern.properties.Properties;
import scala.Option;

public class ZookeeperJobListenerTest {
    
    private TestingServer zkTestServer;
    private ZooKeeper zk;

    private ArrayList<ACL> acls = ZooDefs.Ids.OPEN_ACL_UNSAFE;
    private CreateMode mode = CreateMode.PERSISTENT;
    
    @Before
    public void startZookeeper() throws Exception {
        zkTestServer = new TestingServer(2181);
        Thread.sleep(100);
        
        zk = new ZooKeeper("localhost:2181/", 1000, null);
        zk.create("/exdemon", null, acls, mode);
    }
    
    @Test
    public void sparkConfConstructor() throws Exception {
        SparkConf sparkConfig = new SparkConf();
        sparkConfig.set("spark.streaming.listener.connection_string", "localhost:2181/exdemon");
        
        ZookeeperJobListener listener = new ZookeeperJobListener(sparkConfig);

        SparkListenerApplicationStart event = new SparkListenerApplicationStart(
                                                                    "app_name", 
                                                                    Option.apply("app_id_1234"), 
                                                                    Instant.now().toEpochMilli(), 
                                                                    "user_test", 
                                                                    Option.apply("attempt_id_1"), 
                                                                    Option.empty());
        listener.onApplicationStart(event);
        
        assertEquals("app_id_1234", new String(zk.getData("/exdemon/app/id", false, null)));
    }

    @Test
    public void reportOnApplicationStart() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("connection_string", "localhost:2181/exdemon");
        
        ZookeeperJobListener listener = new ZookeeperJobListener(properties);

        SparkListenerApplicationStart event = new SparkListenerApplicationStart(
                                                                    "app_name", 
                                                                    Option.apply("app_id_1234"), 
                                                                    Instant.now().toEpochMilli(), 
                                                                    "user_test", 
                                                                    Option.apply("attempt_id_1"), 
                                                                    Option.empty());
        listener.onApplicationStart(event);
        
        assertEquals("app_id_1234", new String(zk.getData("/exdemon/app/id", false, null)));
    }
    
    @Test
    public void reportOnBatchStarted() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("connection_string", "localhost:2181/exdemon");
        
        ZookeeperJobListener listener = new ZookeeperJobListener(properties);

        Instant now = Instant.now();
        
        BatchInfo batchInfo = new BatchInfo(
                                        new Time(now.toEpochMilli()), 
                                        null, 
                                        now.toEpochMilli(), 
                                        Option.empty(), 
                                        Option.empty(), 
                                        null);
        StreamingListenerBatchStarted event = new StreamingListenerBatchStarted(batchInfo );
        listener.onBatchStarted(event);
        assertEquals(now.toString(), new String(zk.getData("/exdemon/app/batch/batch_timestamp", false, null)));
        
        batchInfo = new BatchInfo(new Time(now.plus(Duration.ofMinutes(1)).toEpochMilli()), 
                                    null, 
                                    now.plus(Duration.ofMinutes(1)).toEpochMilli(),
                                    Option.empty(), 
                                    Option.empty(), 
                                    null);
        event = new StreamingListenerBatchStarted(batchInfo );
        listener.onBatchStarted(event);
        assertEquals(now.plus(Duration.ofMinutes(1)).toString(), new String(zk.getData("/exdemon/app/batch/batch_timestamp", false, null)));
    }
    
    @After
    public void shutDown() throws IOException, InterruptedException {
        if(zkTestServer != null)
            zkTestServer.close();
        zk.close();
    }
    
}
