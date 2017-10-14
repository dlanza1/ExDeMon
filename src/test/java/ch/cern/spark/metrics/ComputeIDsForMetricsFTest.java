package ch.cern.spark.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Test;

import ch.cern.spark.Properties.PropertiesCache;
import ch.cern.spark.PropertiesTest;
import ch.cern.spark.metrics.monitors.Monitors;
import scala.Tuple2;

public class ComputeIDsForMetricsFTest {

    @Test
    public void oneMonitor() throws Exception{
        PropertiesCache prop = PropertiesTest.mockedExpirable();
        prop.get().setProperty("monitor.ID-1.attribute.key1", "val1");
        prop.get().setProperty("monitor.ID-1.attribute.key2", "val2");
        Monitors monitors = new Monitors(prop, null, null, null);
        
        Metric metric = MetricTest.build();
        
        Iterator<Tuple2<MonitorIDMetricIDs, Metric>> result = new ComputeIDsForMetricsF(monitors).call(metric);
        
        assertResult(result, metric, "ID-1");
        
        assertFalse(result.hasNext());
    }

    @Test
    public void severalMonitors() throws Exception{
        PropertiesCache prop = PropertiesTest.mockedExpirable();
        prop.get().setProperty("monitor.ID-1.filter.attribute.key1", "val1");
        prop.get().setProperty("monitor.ID-1.filter.attribute.key2", "val2");
        prop.get().setProperty("monitor.ID-3.filter.attribute.key1", "val1");
        prop.get().setProperty("monitor.ID-2.filter.attribute.key1", "val1");
        prop.get().setProperty("monitor.ID-3.filter.attribute.key2", "val2");
        prop.get().setProperty("monitor.ID-3.filter.attribute.key3", "val3");
        prop.get().setProperty("monitor.ID-4.filter.attribute.key3", "NO");
        Monitors monitors = new Monitors(prop, null, null, null);
        
        Metric metric = MetricTest.build();
        
        Iterator<Tuple2<MonitorIDMetricIDs, Metric>> result = new ComputeIDsForMetricsF(monitors).call(metric);
        
        assertResult(result, metric, "ID-2");
        assertResult(result, metric, "ID-3");
        assertResult(result, metric, "ID-1");
        
        assertFalse(result.hasNext());
    }
    
    private void assertResult(Iterator<Tuple2<MonitorIDMetricIDs, Metric>> result, Metric metric, String id) {
        assertTrue(result.hasNext());
        
        Tuple2<MonitorIDMetricIDs, Metric> tuple = result.next();
        
        assertEquals(id, tuple._1.getMonitorID());
        assertSame(metric.getIDs(), tuple._1.getMetricIDs());
        
        assertSame(metric, tuple._2);
    }
    
}
