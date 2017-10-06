package ch.cern.spark.metrics.store;

import static org.junit.Assert.*;

import java.util.Date;

import org.apache.spark.streaming.Time;
import org.junit.Test;

public class MetricStoreTest {

    @Test
    public void elapsedTimeWithoutLastestTimestamp(){
        MetricStore store = new MetricStore();
        
        assertEquals(0, store.elapsedTimeFromLastMetric(new Time(1000)));
    }
    
    @Test
    public void elapsedTimeOlder(){
        MetricStore store = new MetricStore();
        
        store.updateLastestTimestamp(new Date(20000));
        
        assertEquals(20, store.elapsedTimeFromLastMetric(new Time(40000)));
    }
    
    @Test
    public void elapsedTimeNewer(){
        MetricStore store = new MetricStore();
        
        store.updateLastestTimestamp(new Date(40000));
        
        assertEquals(0, store.elapsedTimeFromLastMetric(new Time(20000)));
    }
    
}
