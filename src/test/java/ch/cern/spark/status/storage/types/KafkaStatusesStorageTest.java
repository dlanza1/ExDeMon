package ch.cern.spark.status.storage.types;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.utils.ByteArray;
import scala.Tuple2;

public class KafkaStatusesStorageTest {
	
    //@Test
    public void test() throws ConfigurationException {
        
        KafkaStatusesStorage source = new KafkaStatusesStorage();
        Properties properties = new Properties();
        properties.setProperty("topic", "metrics_monitor_checkpoint_qa");
        properties.setProperty("consumer.bootstrap.servers","monit-kafkax-dev.cern.ch:9092");
        source.config(properties);
        
        List<ConsumerRecordSer> records = source.getAllRecords();
        
        System.out.println(records.size());
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
