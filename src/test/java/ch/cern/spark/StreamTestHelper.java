package ch.cern.spark;

import static org.junit.Assert.assertEquals;

import java.io.Serializable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.util.ManualClock;
import org.junit.After;
import org.junit.Before;

import ch.cern.spark.TestInputStream.JTestInputStream;
import ch.cern.spark.metrics.Metric;
import scala.reflect.ClassTag;

public class StreamTestHelper<IN, OUT> implements Serializable {

	private static final long serialVersionUID = -3440438574165569356L;

	private JavaStreamingContext sc = null;
    
    public BatchCounter batchCounter;
    
    private Batches<IN> inputBatches;
    private Batches<OUT> expectedBatches;

    @Before
    public void setUp() throws Exception {        
        Path checkpointPath = new Path("/tmp/spark-checkpoint-tests/");
        FileSystem fs = FileSystem.get(new Configuration());
        fs.delete(checkpointPath, true);
        
        SparkConf sparkConf = new SparkConf();
        sparkConf.setAppName("Test");
        sparkConf.setMaster("local[2]");
        sparkConf.set("spark.driver.host", "localhost");
        sparkConf.set("spark.driver.allowMultipleContexts", "true");
        sparkConf.set("spark.streaming.clock", "org.apache.spark.util.ManualClock");
        sparkConf.set(RDD.CHECKPPOINT_DIR_PARAM, checkpointPath.toString());
        
        sc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
        sc.checkpoint(checkpointPath.toString());
            
        batchCounter = new BatchCounter();
        sc.addStreamingListener(batchCounter);
        
        inputBatches = new Batches<>();
        expectedBatches = new Batches<>();
    }
    
    public void addInput(int batch, IN element) {
        inputBatches.add(batch, element);
    }
    
    public void addExpected(int batch, OUT element) {
        expectedBatches.add(batch, element);
    }
    
    public void assertExpected(Stream<OUT> results) {
        assertEquals(expectedBatches, collect(results));
    }
    
    public Stream<IN> createStream(Class<Metric> class1){
        long batchDuration = getBatchDuration();
        
        ClassTag<IN> classTag = scala.reflect.ClassTag$.MODULE$.apply(class1);
        
        return Stream.from(new JTestInputStream<IN>(sc.ssc(), classTag, inputBatches, batchDuration));
    }
    
    private int getNumBatches() {
        return inputBatches.size();
    }

    public Batches<OUT> collect(Stream<OUT> results) {
        Batches<OUT> outputBatches = new Batches<>();
        
        results.foreachRDD(rdd -> {
                outputBatches.addBatch(rdd.asJavaRDD().collect());
            });
        
        start();
        
        return outputBatches;
    }
    
    public boolean start(){
        sc.start();
        
        ((ManualClock) sc.ssc().scheduler().clock()).advance(getBatchDuration() * getNumBatches() + 1);
        
        return waitUntilBatchesCompleted();
    }
    
    private long getBatchDuration() {
        return sc.ssc().progressListener().batchDuration();
    }

    public boolean waitUntilBatchesCompleted(){
        long now = System.currentTimeMillis();
        long batchDuration = sc.ssc().progressListener().batchDuration();
        long timeout = now + (getNumBatches() * batchDuration) + 5000; 
        
        while(now < timeout) {
            if(batchCounter.getNumCompletedBatches() < getNumBatches()) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {}
            }else {
                return true;
            }
            
            now = System.currentTimeMillis();
        }
        
        return false;
    }
    
    @After
    public void tearDown() {
        if(sc != null)
            sc.stop();
        sc = null;
    }
    
}
