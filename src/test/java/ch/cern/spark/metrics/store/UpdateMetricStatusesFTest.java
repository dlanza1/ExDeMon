package ch.cern.spark.metrics.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.HashMap;

import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;
import org.junit.Test;

import ch.cern.spark.Properties.PropertiesCache;
import ch.cern.spark.PropertiesTest;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.MonitorIDMetricIDs;
import ch.cern.spark.metrics.results.AnalysisResult;

public class UpdateMetricStatusesFTest {

    @Test
    public void timingOutMetric() throws Exception{
        PropertiesCache props = PropertiesTest.mockedExpirable();
        
        UpdateMetricStatusesF func = new UpdateMetricStatusesF(props);
        
        Time time = new Time(1000);
        MonitorIDMetricIDs ids = new MonitorIDMetricIDs("ID", new HashMap<String, String>());
        Optional<Metric> metricOpt = null;
        @SuppressWarnings("unchecked")
        State<MetricStore> storeState = mock(State.class);
        when(storeState.isTimingOut()).thenReturn(true);
        
        Optional<AnalysisResult> resultOpt = func.call(time, ids, metricOpt, storeState);
        
        assertTrue(resultOpt.isPresent());
        assertEquals(AnalysisResult.Status.EXCEPTION, resultOpt.get().getStatus());
        assertEquals("Metric has timmed out.", resultOpt.get().getStatusReason());
    }
    
    @Test
    public void noMetric() throws Exception{
        PropertiesCache props = PropertiesTest.mockedExpirable();
        
        UpdateMetricStatusesF func = new UpdateMetricStatusesF(props);
        
        Time time = new Time(1000);
        MonitorIDMetricIDs ids = new MonitorIDMetricIDs("ID", new HashMap<String, String>());
        Optional<Metric> metricOpt = Optional.absent();
        @SuppressWarnings("unchecked")
        State<MetricStore> storeState = mock(State.class);
        
        Optional<AnalysisResult> resultOpt = func.call(time, ids, metricOpt, storeState);
        
        assertFalse(resultOpt.isPresent());
    }
    
    @Test
    public void updateLastestTimestamp() throws Exception{
        PropertiesCache props = PropertiesTest.mockedExpirable();
        props.get().setProperty("monitor.ID.analysis.type", "fixed-threshold");
        
        UpdateMetricStatusesF func = new UpdateMetricStatusesF(props);
        
        MonitorIDMetricIDs ids = new MonitorIDMetricIDs("ID", new HashMap<String, String>());
        Instant metricTime = Instant.ofEpochMilli(Instant.now().toEpochMilli());        
        Optional<Metric> metricOpt = Optional.of(new Metric(metricTime , 10, new HashMap<String, String>()));
        
        @SuppressWarnings("unchecked")
        State<MetricStore> storeState = mock(State.class);
        MetricStore store = new MetricStore();
        when(storeState.exists()).thenReturn(true);
        when(storeState.get()).thenReturn(store);
        
        Optional<AnalysisResult> resultOpt = func.call(new Time(1000), ids, metricOpt, storeState);
        
        assertTrue(resultOpt.isPresent());
        verify(storeState, times(1)).update(store);
        assertEquals(metricTime, store.getLastestTimestamp().get());
    }
    
}
