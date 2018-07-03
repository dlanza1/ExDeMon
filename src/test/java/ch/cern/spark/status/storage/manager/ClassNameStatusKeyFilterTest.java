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

public class ClassNameStatusKeyFilterTest {

    @Test
    public void filterByClassName() throws Exception {
        ClassNameStatusKeyFilter filter = new ClassNameStatusKeyFilter(DefinedMetricStatuskey.class.getName());
        
        assertTrue(filter.call(new Tuple2<>(new DefinedMetricStatuskey("dm1", new HashMap<>()), new TestStatus(1))));
        assertTrue(filter.call(new Tuple2<>(new DefinedMetricStatuskey("dm2", new HashMap<>()), new TestStatus(1))));
        
        assertFalse(filter.call(new Tuple2<>(new MonitorStatusKey("m1", new HashMap<>()), new TestStatus(1))));
        assertFalse(filter.call(new Tuple2<>(new MonitorStatusKey("m1", new HashMap<>()), new TestStatus(1))));
        assertFalse(filter.call(new Tuple2<>(new MonitorStatusKey("m2", new HashMap<>()), new TestStatus(1))));
        assertFalse(filter.call(new Tuple2<>(new TriggerStatusKey("m1", "n1", new HashMap<>()), new TestStatus(1))));
        assertFalse(filter.call(new Tuple2<>(new TriggerStatusKey("m1", "n2", new HashMap<>()), new TestStatus(1))));
    }
    
    @Test
    public void filterByAliasClassName() throws Exception {
        ClassNameStatusKeyFilter filter = new ClassNameStatusKeyFilter("defined-metric-key");
        
        assertTrue(filter.call(new Tuple2<>(new DefinedMetricStatuskey("dm1", new HashMap<>()), new TestStatus(1))));
        assertTrue(filter.call(new Tuple2<>(new DefinedMetricStatuskey("dm2", new HashMap<>()), new TestStatus(1))));
        
        assertFalse(filter.call(new Tuple2<>(new MonitorStatusKey("m1", new HashMap<>()), new TestStatus(1))));
        assertFalse(filter.call(new Tuple2<>(new MonitorStatusKey("m1", new HashMap<>()), new TestStatus(1))));
        assertFalse(filter.call(new Tuple2<>(new MonitorStatusKey("m2", new HashMap<>()), new TestStatus(1))));
        assertFalse(filter.call(new Tuple2<>(new TriggerStatusKey("m1", "n1", new HashMap<>()), new TestStatus(1))));
        assertFalse(filter.call(new Tuple2<>(new TriggerStatusKey("m1", "n2", new HashMap<>()), new TestStatus(1))));
    }

}
