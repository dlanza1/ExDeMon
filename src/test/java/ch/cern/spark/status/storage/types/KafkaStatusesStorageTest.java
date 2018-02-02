package ch.cern.spark.status.storage.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.kafka010.KafkaTestUtils;
import org.junit.Test;

import ch.cern.properties.Properties;
import ch.cern.spark.SparkConf;
import ch.cern.spark.metrics.defined.DefinedMetricStatuskey;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import ch.cern.spark.metrics.monitors.MonitorStatusKey;
import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.TestStatus;
import ch.cern.utils.ByteArray;
import scala.Tuple2;

public class KafkaStatusesStorageTest {
    
    private transient JavaSparkContext context = null;

    private transient KafkaTestUtils kafkaTestUtils;
    
    public void setUp(String topic) throws Exception {
        if(kafkaTestUtils == null) {
            kafkaTestUtils = new KafkaTestUtils();
            kafkaTestUtils.setup();
        }

        kafkaTestUtils.createTopic(topic);
        
        if(context == null) {
            SparkConf sparkConf = new SparkConf();
            sparkConf.setAppName("Test");
            sparkConf.setMaster("local[2]");
            sparkConf.set("spark.driver.host", "localhost");
            sparkConf.set("spark.driver.allowMultipleContexts", "true");
            
            context = new JavaSparkContext(sparkConf);
        }
    }
	
	@Test
	public void shouldSaveAndLoadSameData() throws Exception {
	    String topic = "shouldSaveAndLoadSameData";
	    setUp(topic);
	    
		KafkaStatusesStorage storage = new KafkaStatusesStorage();
		
		Properties properties = new Properties();
		properties.setProperty("topic", topic);
		properties.setProperty("producer.bootstrap.servers", kafkaTestUtils.brokerAddress());
		properties.setProperty("consumer.bootstrap.servers", kafkaTestUtils.brokerAddress());
		properties.setProperty("consumer.group.id", "testing1");
		storage.config(properties);
		
		List<Tuple2<DefinedMetricStatuskey, VariableStatuses>> inputList = new LinkedList<>();
		DefinedMetricStatuskey id = new DefinedMetricStatuskey("df1", new HashMap<>());
		VariableStatuses statuses = new VariableStatuses();
		statuses.newProcessedBatchTime(Instant.now());
		statuses.put("v1", new TestStatus(100));
		statuses.put("v2", new TestStatus(101));
		inputList.add(new Tuple2<DefinedMetricStatuskey, VariableStatuses>(id, statuses));
		
		JavaPairRDD<DefinedMetricStatuskey, VariableStatuses> inputRDD = context.parallelize(inputList).mapToPair(f->f);
		storage.save(inputRDD, new Time(0));
		
		JavaPairRDD<DefinedMetricStatuskey, VariableStatuses> outputRDD = storage.load(context, DefinedMetricStatuskey.class, VariableStatuses.class);
		List<Tuple2<DefinedMetricStatuskey, VariableStatuses>> outputList = outputRDD.collect();
		
		assertNotSame(inputList, outputList);
		assertEquals(inputList, outputList);
	}
	
    @Test
    public void shouldLoadDifferentKeys() throws Exception {
        String topic = "shouldLoadDifferentKeys";
        setUp(topic);
        
        KafkaStatusesStorage storage = new KafkaStatusesStorage();

        Properties properties = new Properties();
        properties.setProperty("topic", topic);
        properties.setProperty("producer.bootstrap.servers", kafkaTestUtils.brokerAddress());
        properties.setProperty("consumer.bootstrap.servers", kafkaTestUtils.brokerAddress());
        properties.setProperty("consumer.group.id", "testing2");
        storage.config(properties);

        List<Tuple2<StatusKey, StatusValue>> inputList = new LinkedList<>();

        StatusKey id = new DefinedMetricStatuskey("df1", new HashMap<>());
        StatusValue status = new TestStatus(14);
        Tuple2<StatusKey, StatusValue> tuple1 = new Tuple2<StatusKey, StatusValue>(id, status);
        inputList.add(tuple1);
        JavaPairRDD<StatusKey, StatusValue> inputRDD = context.parallelize(inputList).mapToPair(f -> f);
        storage.save(inputRDD, new Time(0));

        inputList = new LinkedList<>();
        id = new MonitorStatusKey("mon1", new HashMap<>());
        status = new TestStatus(10);
        Tuple2<StatusKey, StatusValue> tuple2 = new Tuple2<StatusKey, StatusValue>(id, status);
        inputList.add(tuple2);
        inputRDD = context.parallelize(inputList).mapToPair(f -> f);

        storage.save(inputRDD, new Time(0));

        JavaPairRDD<DefinedMetricStatuskey, TestStatus> outputRDD = storage.load(context, DefinedMetricStatuskey.class, TestStatus.class);
        List<Tuple2<DefinedMetricStatuskey, TestStatus>> outputList = outputRDD.collect();

        JavaPairRDD<MonitorStatusKey, TestStatus> outputRDD2 = storage.load(context, MonitorStatusKey.class, TestStatus.class);
        List<Tuple2<MonitorStatusKey, TestStatus>> outputList2 = outputRDD2.collect();

        assertEquals(tuple1, outputList.get(0));
        assertEquals(tuple2, outputList2.get(0));
    }
	
	@Test
	public void shouldGetLastRecord() throws Exception {
	    LinkedList<Tuple2<Long, ByteArray>> records = new LinkedList<>();
	    records.add(new Tuple2<Long, ByteArray>(0L, new ByteArray("0".getBytes())));
	    records.add(new Tuple2<Long, ByteArray>(1L, new ByteArray("1".getBytes())));
	    
	    Tuple2<Long, ByteArray> lastRecord = new Tuple2<Long, ByteArray>(10L, new ByteArray("10".getBytes()));
        records.add(lastRecord);

        Tuple2<ByteArray, Iterable<Tuple2<Long, ByteArray>>> pair = new Tuple2<>(new ByteArray("key".getBytes()), records);
        Tuple2<ByteArray, ByteArray> actualLast = KafkaStatusesStorage.getLastRecord(pair);

		assertSame(lastRecord._2, actualLast._2);
	}
	
    @Test
    public void shouldGetLastRecordWhenValueNull() throws Exception {
        LinkedList<Tuple2<Long, ByteArray>> records = new LinkedList<>();
        records.add(new Tuple2<Long, ByteArray>(0L, new ByteArray("0".getBytes())));
        records.add(new Tuple2<Long, ByteArray>(1L, new ByteArray("1".getBytes())));
        records.add(new Tuple2<Long, ByteArray>(10L, null));

        Tuple2<ByteArray, Iterable<Tuple2<Long, ByteArray>>> pair = new Tuple2<>(new ByteArray("key".getBytes()), records);
        Tuple2<ByteArray, ByteArray> actualLast = KafkaStatusesStorage.getLastRecord(pair);

        assertNull(actualLast._2);
    }
	
}
