package ch.cern.spark.metrics.store;

import static org.junit.Assert.assertEquals;

import java.time.Instant;

import org.junit.Test;

public class MetricStoreTest {

    @Test
    public void elapsedTimeWithoutLastestTimestamp(){
        MetricStore store = new MetricStore();
        
        assertEquals(0, store.elapsedTimeFromLastMetric(Instant.now()).getSeconds());
    }
    
    @Test
    public void elapsedTimeOlder(){
        MetricStore store = new MetricStore();

		store.updateLastestTimestamp(Instant.ofEpochSecond(20));
        
        assertEquals(20, store.elapsedTimeFromLastMetric(Instant.ofEpochSecond(40)).getSeconds());
    }

	@Test
    public void elapsedTimeNewer(){
        MetricStore store = new MetricStore();
        
        store.updateLastestTimestamp(Instant.ofEpochSecond(40));
        
        assertEquals(20, store.elapsedTimeFromLastMetric(Instant.ofEpochSecond(20)).getSeconds());
    }
    
}
