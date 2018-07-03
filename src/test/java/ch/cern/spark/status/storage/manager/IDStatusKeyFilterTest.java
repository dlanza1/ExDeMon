package ch.cern.spark.status.storage.manager;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;

import org.junit.Test;

import ch.cern.exdemon.metrics.defined.DefinedMetricStatuskey;
import ch.cern.exdemon.monitor.MonitorStatusKey;
import ch.cern.exdemon.monitor.trigger.TriggerStatusKey;
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
        assertFalse(filter.call(new Tuple2<>(new TriggerStatusKey("m1", "n1", new HashMap<>()), new TestStatus(1))));
        
        filter = new IDStatusKeyFilter("dm2:2");
        
        assertFalse(filter.call(new Tuple2<>(new TriggerStatusKey("dm1", "2", new HashMap<>()), new TestStatus(1))));
        assertTrue(filter.call(new Tuple2<>(new TriggerStatusKey("dm2", "2", new HashMap<>()), new TestStatus(1))));
    }

}
