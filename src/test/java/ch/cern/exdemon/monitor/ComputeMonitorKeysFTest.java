package ch.cern.exdemon.monitor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;

import org.junit.Before;
import org.junit.Test;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.MetricTest;
import ch.cern.properties.Properties;
import scala.Tuple2;

public class ComputeMonitorKeysFTest {

    @Before
    public void reset() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("type", "test");
        ComponentsCatalog.init(properties);
        ComponentsCatalog.reset();
    }

    @Test
    public void oneMonitor() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("analysis.type", "true");
        properties.setProperty("filter.attribute.key1", "val1");
        properties.setProperty("filter.attribute.key2", "val2");
        ComponentsCatalog.register(Type.MONITOR, "ID-1", properties);

        Metric metric = MetricTest.build();

        Iterator<Tuple2<MonitorStatusKey, Metric>> result = new ComputeMonitorKeysF(null).call(metric);

        assertResult(result, metric, "ID-1");

        assertFalse(result.hasNext());
    }

    @Test
    public void severalMonitors() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("analysis.type", "true");
        properties.setProperty("filter.attribute.key1", "val1");
        properties.setProperty("filter.attribute.key2", "val2");
        ComponentsCatalog.register(Type.MONITOR, "ID-1", properties);
        
        properties = new Properties();
        properties.setProperty("analysis.type", "true");
        properties.setProperty("filter.attribute.key1", "val1");
        ComponentsCatalog.register(Type.MONITOR, "ID-2", properties);
        
        properties = new Properties();
        properties.setProperty("analysis.type", "true");
        properties.setProperty("filter.attribute.key2", "val2");
        properties.setProperty("filter.attribute.key3", "val3");
        ComponentsCatalog.register(Type.MONITOR, "ID-3", properties);
        
        properties = new Properties();
        properties.setProperty("analysis.type", "true");
        properties.setProperty("filter.attribute.key3", "NO");
        ComponentsCatalog.register(Type.MONITOR, "ID-4", properties);

        Metric metric = MetricTest.build();

        Iterator<Tuple2<MonitorStatusKey, Metric>> result = new ComputeMonitorKeysF(null).call(metric);

        assertResult(result, metric, "ID-2");
        assertResult(result, metric, "ID-3");
        assertResult(result, metric, "ID-1");

        assertFalse(result.hasNext());
    }

    private void assertResult(Iterator<Tuple2<MonitorStatusKey, Metric>> result, Metric metric, String id) {
        assertTrue(result.hasNext());

        Tuple2<MonitorStatusKey, Metric> tuple = result.next();

        assertEquals(id, tuple._1.getID());
        assertSame(metric.getAttributes(), tuple._1.getMetric_attributes());

        assertSame(metric, tuple._2);
    }

}
