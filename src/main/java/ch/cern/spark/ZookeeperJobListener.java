package ch.cern.spark;

import java.time.Instant;
import java.util.Arrays;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.log4j.Logger;
import org.apache.spark.scheduler.SparkListenerApplicationEnd;
import org.apache.spark.scheduler.SparkListenerApplicationStart;
import org.apache.spark.scheduler.SparkListenerBlockManagerAdded;
import org.apache.spark.scheduler.SparkListenerBlockManagerRemoved;
import org.apache.spark.scheduler.SparkListenerBlockUpdated;
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate;
import org.apache.spark.scheduler.SparkListenerEvent;
import org.apache.spark.scheduler.SparkListenerExecutorAdded;
import org.apache.spark.scheduler.SparkListenerExecutorBlacklisted;
import org.apache.spark.scheduler.SparkListenerExecutorMetricsUpdate;
import org.apache.spark.scheduler.SparkListenerExecutorRemoved;
import org.apache.spark.scheduler.SparkListenerExecutorUnblacklisted;
import org.apache.spark.scheduler.SparkListenerInterface;
import org.apache.spark.scheduler.SparkListenerJobEnd;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.scheduler.SparkListenerNodeBlacklisted;
import org.apache.spark.scheduler.SparkListenerNodeUnblacklisted;
import org.apache.spark.scheduler.SparkListenerSpeculativeTaskSubmitted;
import org.apache.spark.scheduler.SparkListenerStageCompleted;
import org.apache.spark.scheduler.SparkListenerStageSubmitted;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.scheduler.SparkListenerTaskGettingResult;
import org.apache.spark.scheduler.SparkListenerTaskStart;
import org.apache.spark.scheduler.SparkListenerUnpersistRDD;
import org.apache.spark.streaming.scheduler.StreamingListener;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerBatchSubmitted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationCompleted;
import org.apache.spark.streaming.scheduler.StreamingListenerOutputOperationStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverError;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStarted;
import org.apache.spark.streaming.scheduler.StreamingListenerReceiverStopped;
import org.apache.spark.streaming.scheduler.StreamingListenerStreamingStarted;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import org.apache.spark.SparkConf;

import ch.cern.properties.Properties;
import scala.Option;
import scala.Tuple2;

public class ZookeeperJobListener implements SparkListenerInterface, StreamingListener {
    
    private transient final static Logger LOG = Logger.getLogger(ZookeeperJobListener.class.getName());
    
    private CuratorFramework client;

    private String path;
    
    public ZookeeperJobListener(SparkConf sparkConf) {
        Tuple2<String, String>[] tuples = sparkConf.getAllWithPrefix("spark.streaming.listener.");
        
        Properties props = new Properties();
        props.putAll(Arrays.stream(tuples).collect(Collectors.toMap(Tuple2::_1, Tuple2::_2)));
        
        init(props);
    }

    public ZookeeperJobListener(Properties properties) throws Exception {
        init(properties);
    }
    
    private void init(Properties properties) {
        String connectionString = properties.getProperty("connection_string");
        path = properties.getProperty("path", "/app");
        
        try {
            client = CuratorFrameworkFactory.builder()
                    .connectString(connectionString)
                    .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                    .sessionTimeoutMs(20000)
                    .build();
            client.start();
        }catch(Exception e) {
            LOG.error(e.getMessage(), e);
            
            client = null;
        }
    }

    @Override
    public void onApplicationStart(SparkListenerApplicationStart event) {
        Option<String> idOpt = event.appId();
        if(idOpt.isDefined())
            report(path + "/id", idOpt.get());
        
        Option<String> attemptIdOpt = event.appAttemptId();
        if(attemptIdOpt.isDefined())
            report(path + "/attemptId", attemptIdOpt.get());
        
        report(path + "/status", "RUNNING");
        report(path + "/start_timestamp", Instant.now().toString());
    }

    @Override
    public void onApplicationEnd(SparkListenerApplicationEnd event) {
        report(path + "/status", "FINISHED");
        report(path + "/end_timestamp", Instant.now().toString());
    } 

    @Override
    public void onBatchStarted(StreamingListenerBatchStarted event) {
        report(path + "/batch/batch_timestamp", Instant.ofEpochMilli(event.batchInfo().batchTime().milliseconds()).toString());
        report(path + "/batch/start_timestamp", Instant.now().toString());
    }
    
    @Override
    public void onBatchCompleted(StreamingListenerBatchCompleted event) {
        report(path + "/batch/end_timestamp", Instant.now().toString());
    }
    
    @Override
    public void onStreamingStarted(StreamingListenerStreamingStarted event) {
    }
    
    @Override
    public void onBatchSubmitted(StreamingListenerBatchSubmitted event) {
    }

    @Override
    public void onOutputOperationCompleted(StreamingListenerOutputOperationCompleted event) {
    }

    @Override
    public void onOutputOperationStarted(StreamingListenerOutputOperationStarted event) {
    }

    @Override
    public void onReceiverError(StreamingListenerReceiverError event) {
    }

    @Override
    public void onReceiverStarted(StreamingListenerReceiverStarted event) {
    }

    @Override
    public void onReceiverStopped(StreamingListenerReceiverStopped event) {
    }

    @Override
    public void onBlockManagerAdded(SparkListenerBlockManagerAdded event) {
    }

    @Override
    public void onBlockManagerRemoved(SparkListenerBlockManagerRemoved event) {
    }

    @Override
    public void onBlockUpdated(SparkListenerBlockUpdated event) {
    }

    @Override
    public void onEnvironmentUpdate(SparkListenerEnvironmentUpdate event) {
    }

    @Override
    public void onExecutorAdded(SparkListenerExecutorAdded event) {
    }

    @Override
    public void onExecutorBlacklisted(SparkListenerExecutorBlacklisted event) {
    }

    @Override
    public void onExecutorMetricsUpdate(SparkListenerExecutorMetricsUpdate event) {
    }

    @Override
    public void onExecutorRemoved(SparkListenerExecutorRemoved event) {
    }

    @Override
    public void onExecutorUnblacklisted(SparkListenerExecutorUnblacklisted event) {
    }

    @Override
    public void onJobEnd(SparkListenerJobEnd event) {
    }

    @Override
    public void onJobStart(SparkListenerJobStart event) {   
    }

    @Override
    public void onNodeBlacklisted(SparkListenerNodeBlacklisted event) {
    }

    @Override
    public void onNodeUnblacklisted(SparkListenerNodeUnblacklisted event) {
    }

    @Override
    public void onOtherEvent(SparkListenerEvent event) {
    }

    @Override
    public void onStageCompleted(SparkListenerStageCompleted event) {
    }

    @Override
    public void onStageSubmitted(SparkListenerStageSubmitted event) {
    }

    @Override
    public void onTaskEnd(SparkListenerTaskEnd event) {
    }

    @Override
    public void onTaskGettingResult(SparkListenerTaskGettingResult event) {
    }

    @Override
    public void onTaskStart(SparkListenerTaskStart event) {
    }

    @Override
    public void onUnpersistRDD(SparkListenerUnpersistRDD event) {
    }

    @Override
    public void onSpeculativeTaskSubmitted(SparkListenerSpeculativeTaskSubmitted arg0) {
    }
    
    private void report(String path, String value) {
        if(client == null)
            return;
        
        try {
            client.setData().forPath(path, value.getBytes());
        }catch(Throwable e1) {
            try {
                client.create()
                        .creatingParentsIfNeeded()
                        .withMode(CreateMode.PERSISTENT)
                        .withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
                        .forPath(path, value.getBytes());
            }catch(Throwable e2) {
                LOG.error(e2.getMessage(), e2);
            }
        }
    }

}
