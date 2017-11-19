package ch.cern.spark.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.monitors.Monitors;
import scala.Tuple2;

public class ComputeIDsForMetricsFTest {
	
	private Cache<Properties> propertiesCache = Properties.getCache();
	
	@Before
	public void reset() throws ConfigurationException {
		Properties.initCache(null);
		propertiesCache = Properties.getCache();
		propertiesCache.reset();
		Monitors.getCache().reset();
	}

    @Test
    public void oneMonitor() throws Exception{
    		propertiesCache.get().setProperty("monitor.ID-1.analysis.type", "true");
    		propertiesCache.get().setProperty("monitor.ID-1.filter.attribute.key1", "val1");
    		propertiesCache.get().setProperty("monitor.ID-1.filter.attribute.key2", "val2");
        
        Metric metric = MetricTest.build();
        
        Iterator<Tuple2<MonitorIDMetricIDs, Metric>> result = new ComputeIDsForMetricsF(null).call(metric);
        
        assertResult(result, metric, "ID-1");
        
        assertFalse(result.hasNext());
    }

    @Test
    public void severalMonitors() throws Exception{
    		propertiesCache.get().setProperty("monitor.ID-1.analysis.type", "true");
    		propertiesCache.get().setProperty("monitor.ID-1.filter.attribute.key1", "val1");
    		propertiesCache.get().setProperty("monitor.ID-1.filter.attribute.key2", "val2");
    		propertiesCache.get().setProperty("monitor.ID-2.analysis.type", "true");
    		propertiesCache.get().setProperty("monitor.ID-2.filter.attribute.key1", "val1");
    		propertiesCache.get().setProperty("monitor.ID-3.analysis.type", "true");
    		propertiesCache.get().setProperty("monitor.ID-3.filter.attribute.key2", "val2");
    		propertiesCache.get().setProperty("monitor.ID-3.filter.attribute.key3", "val3");
    		propertiesCache.get().setProperty("monitor.ID-4.analysis.type", "true");
    		propertiesCache.get().setProperty("monitor.ID-4.filter.attribute.key3", "NO");
        
        Metric metric = MetricTest.build();
        
        Iterator<Tuple2<MonitorIDMetricIDs, Metric>> result = new ComputeIDsForMetricsF(null).call(metric);
        
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
