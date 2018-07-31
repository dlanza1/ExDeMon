package ch.cern.exdemon.monitor;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateImpl;
import org.junit.Before;
import org.junit.Test;

import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.properties.Properties;
import ch.cern.spark.status.StatusValue;

public class MonitorTest {

    @Before
    public void setUp() throws Exception {
        ComponentsCatalog.reset();
    }

    @Test
    public void fixedValuesAttributes() throws Exception {

        Properties properties = new Properties();
        properties.setProperty("analysis.type", "fixed-threshold");
        properties.setProperty("analysis.error.upperbound", "20");
        properties.setProperty("analysis.error.lowerbound", "10");
        properties.setProperty("attribute.monitor", "monitor_name");
        properties.setProperty("attribute.other", "other_value");
        Monitor monitor = new Monitor("test");
        monitor.config(properties);

        State<StatusValue> store = new StateImpl<>();

        AnalysisResult result = monitor.process(store, new Metric(Instant.now(), 0f, new HashMap<>())).get();
        assertEquals("monitor_name", result.getAnalyzed_metric().getAttributes().get("monitor"));
        assertEquals("other_value", result.getAnalyzed_metric().getAttributes().get("other"));
    }

    @Test
    public void tagsShouldBePropagated() throws Exception {

        Properties properties = new Properties();
        properties.setProperty("analysis.type", "fixed-threshold");
        properties.setProperty("analysis.error.upperbound", "20");
        properties.setProperty("analysis.error.lowerbound", "10");
        properties.setProperty("tags.email", "1234@cern.ch");
        properties.setProperty("tags.group", "IT_DB");
        Monitor monitor = new Monitor("test");
        monitor.config(properties);

        State<StatusValue> store = new StateImpl<>();

        AnalysisResult result = monitor.process(store, new Metric(Instant.now(), 0f, new HashMap<>())).get();
        assertEquals(AnalysisResult.Status.ERROR, result.getStatus());
        assertEquals("1234@cern.ch", result.getTags().get("email"));
        assertEquals("IT_DB", result.getTags().get("group"));

        result = monitor.process(store, new Metric(Instant.now(), 15f, new HashMap<>())).get();
        assertEquals(AnalysisResult.Status.OK, result.getStatus());
        assertEquals("1234@cern.ch", result.getTags().get("email"));
        assertEquals("IT_DB", result.getTags().get("group"));
    }

    @Test
    public void tagsShouldExtractMetricAttributes() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("analysis.type", "fixed-threshold");
        properties.setProperty("analysis.error.upperbound", "20");
        properties.setProperty("analysis.error.lowerbound", "10");
        properties.setProperty("tags.email", "1234@cern.ch");
        properties.setProperty("tags.group", "IT_DB");
        properties.setProperty("tags.sink-conf-id.target", "%target.conf");
        Monitor monitor = new Monitor("test");
        monitor.config(properties);

        State<StatusValue> store = new StateImpl<>();

        Map<String, String> metricIds = new HashMap<>();
        metricIds.put("target.conf", "target-in-metric1");
        AnalysisResult result = monitor.process(store, new Metric(Instant.now(), 0f, metricIds)).get();
        assertEquals(AnalysisResult.Status.ERROR, result.getStatus());
        assertEquals("1234@cern.ch", result.getTags().get("email"));
        assertEquals("IT_DB", result.getTags().get("group"));
        assertEquals("target-in-metric1", result.getTags().get("sink-conf-id.target"));

        metricIds = new HashMap<>();
        metricIds.put("target.conf", "target-in-metric2");
        result = monitor.process(store, new Metric(Instant.now(), 15f, metricIds)).get();
        assertEquals(AnalysisResult.Status.OK, result.getStatus());
        assertEquals("1234@cern.ch", result.getTags().get("email"));
        assertEquals("IT_DB", result.getTags().get("group"));
        assertEquals("target-in-metric2", result.getTags().get("sink-conf-id.target"));
    }

}
