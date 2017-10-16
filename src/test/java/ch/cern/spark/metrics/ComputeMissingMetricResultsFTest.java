package ch.cern.spark.metrics;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Iterator;

import org.apache.spark.streaming.Time;
import org.junit.Test;

import ch.cern.spark.Properties;
import ch.cern.spark.PropertiesTest;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.store.MetricStore;
import scala.Tuple2;

public class ComputeMissingMetricResultsFTest {
    
    @Test
    public void noMissingMetric() throws Exception{
        Properties.PropertiesCache propExp = PropertiesTest.mockedExpirable();
        propExp.get().setProperty("monitor.ID.missing.max-period", "60");
        Monitors monitors = new Monitors(propExp);
        
        ComputeMissingMetricResultsF func = new ComputeMissingMetricResultsF(monitors, new Time(50000));
        
        MetricStore metricStore = new MetricStore();
        metricStore.updateLastestTimestamp(Instant.ofEpochSecond(20));
        
        MonitorIDMetricIDs ids = new MonitorIDMetricIDs("ID", null);
        Tuple2<MonitorIDMetricIDs, MetricStore> tuple = new Tuple2<MonitorIDMetricIDs, MetricStore>(ids , metricStore);
        Iterator<AnalysisResult> resultIt = func.call(tuple);
        
        assertFalse(resultIt.hasNext());
    }

    @Test
    public void missingMetric() throws Exception{
        Properties.PropertiesCache propExp = PropertiesTest.mockedExpirable();
        propExp.get().setProperty("monitor.ID.missing.max-period", "10");
        Monitors monitors = new Monitors(propExp);
        
        ComputeMissingMetricResultsF func = new ComputeMissingMetricResultsF(monitors, new Time(50000));
        
        MetricStore metricStore = new MetricStore();
        metricStore.updateLastestTimestamp(Instant.ofEpochSecond(20));
        
        MonitorIDMetricIDs ids = new MonitorIDMetricIDs("ID", null);
        Tuple2<MonitorIDMetricIDs, MetricStore> tuple = new Tuple2<MonitorIDMetricIDs, MetricStore>(ids, metricStore);
        Iterator<AnalysisResult> resultIt = func.call(tuple);
        
        assertResult(resultIt, 30);
    }
    
    private void assertResult(Iterator<AnalysisResult> resultIt, Integer delay) {
        assertTrue(resultIt.hasNext());
        
        AnalysisResult result = resultIt.next();
        
        assertEquals(AnalysisResult.Status.EXCEPTION, result.getStatus());
        assertEquals("Metric missing for " + delay + " seconds.", result.getStatusReason());
        
        assertFalse(resultIt.hasNext());
    }

}
