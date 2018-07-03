package ch.cern.spark.status.storage.manager;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import ch.cern.exdemon.metrics.defined.DefinedMetricStatuskey;
import ch.cern.exdemon.monitor.MonitorStatusKey;
import ch.cern.exdemon.monitor.trigger.TriggerStatusKey;
import ch.cern.spark.status.TestStatus;
import scala.Tuple2;

public class ToStringPatternStatusKeyFilterTest {
    
    @Test
    public void filterByID() throws Exception {
        ToStringPatternStatusKeyFilter filter = new ToStringPatternStatusKeyFilter(".*tpsrv12.*");
        
        assertFalse(filter.call(new Tuple2<>(new DefinedMetricStatuskey("dm1", new HashMap<>()), new TestStatus(1))));
        
        assertTrue(filter.call(new Tuple2<>(new DefinedMetricStatuskey("tpsrv12", new HashMap<>()), new TestStatus(1))));
        
        Map<String, String> att = new HashMap<>();
        att.put("", "tpsrv12");
        assertTrue(filter.call(new Tuple2<>(new MonitorStatusKey("m1", att), new TestStatus(1))));
        
        att = new HashMap<>();
        att.put("", "tpsrv12");
        assertTrue(filter.call(new Tuple2<>(new TriggerStatusKey("m1", "dm2", att), new TestStatus(1))));
    }

}
