package ch.cern.spark.status.storage.manager;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.Test;

import ch.cern.spark.metrics.defined.DefinedMetricStatuskey;
import ch.cern.spark.metrics.monitors.MonitorStatusKey;
import ch.cern.spark.metrics.notificator.NotificatorStatusKey;
import ch.cern.spark.status.TestStatus;
import scala.Tuple2;

public class IDStatusKeyFilterTest {
    
    @Test
    public void filterByID() throws Exception {
        IDStatusKeyFilter filter = new IDStatusKeyFilter("dm2");
        
        assertFalse(filter.call(new Tuple2<>(new DefinedMetricStatuskey("dm1", new HashMap<>()), new TestStatus(1))));
        assertTrue(filter.call(new Tuple2<>(new DefinedMetricStatuskey("dm2", new HashMap<>()), new TestStatus(1))));
        assertFalse(filter.call(new Tuple2<>(new MonitorStatusKey("m1", new HashMap<>()), new TestStatus(1))));
        assertTrue(filter.call(new Tuple2<>(new MonitorStatusKey("dm2", new HashMap<>()), new TestStatus(1))));
        assertFalse(filter.call(new Tuple2<>(new NotificatorStatusKey("m1", "n1", new HashMap<>()), new TestStatus(1))));
        
        filter = new IDStatusKeyFilter("dm2:2");
        
        assertFalse(filter.call(new Tuple2<>(new NotificatorStatusKey("dm1", "2", new HashMap<>()), new TestStatus(1))));
        assertTrue(filter.call(new Tuple2<>(new NotificatorStatusKey("dm2", "2", new HashMap<>()), new TestStatus(1))));
    }

}
