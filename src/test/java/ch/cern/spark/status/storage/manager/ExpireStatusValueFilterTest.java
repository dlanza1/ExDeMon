package ch.cern.spark.status.storage.manager;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;

import org.apache.spark.streaming.StateImpl;
import org.apache.spark.streaming.Time;
import org.junit.Test;

import ch.cern.exdemon.metrics.defined.DefinedMetricStatuskey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.TestStatus;
import scala.Tuple2;

public class ExpireStatusValueFilterTest {
    
    @Test
    public void filterByExpiration() throws Exception {
        ExpireStatusValueFilter filter = new ExpireStatusValueFilter(Duration.ofMinutes(4));
        
        Instant now = Instant.now();
        
        assertTrue(filter.call(new Tuple2<>(new DefinedMetricStatuskey("dm1", new HashMap<>()), new TestStatus(1))));
        
        StatusValue status = new TestStatus(1);
        status.update(new StateImpl<>(), new Time(now.minus(Duration.ofMinutes(10)).toEpochMilli()));
        assertTrue(filter.call(new Tuple2<>(new DefinedMetricStatuskey("dm1", new HashMap<>()), status)));
        
        status = new TestStatus(1);
        status.update(new StateImpl<>(), new Time(now.minus(Duration.ofMinutes(5)).toEpochMilli()));
        assertTrue(filter.call(new Tuple2<>(new DefinedMetricStatuskey("dm1", new HashMap<>()), status)));
        
        status = new TestStatus(1);
        status.update(new StateImpl<>(), new Time(now.minus(Duration.ofMinutes(3)).toEpochMilli()));
        assertFalse(filter.call(new Tuple2<>(new DefinedMetricStatuskey("dm1", new HashMap<>()), status)));
        
        status = new TestStatus(1);
        status.update(new StateImpl<>(), new Time(now.minus(Duration.ofMinutes(1)).toEpochMilli()));
        assertFalse(filter.call(new Tuple2<>(new DefinedMetricStatuskey("dm1", new HashMap<>()), status)));
    }

}
