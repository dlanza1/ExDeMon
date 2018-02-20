package ch.cern.spark.status.storage.types;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;

import java.util.LinkedList;

import org.junit.Test;

import ch.cern.utils.ByteArray;
import scala.Tuple2;

public class KafkaStatusesStorageTest {
	
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
