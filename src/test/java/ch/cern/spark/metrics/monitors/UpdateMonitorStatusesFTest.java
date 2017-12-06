package ch.cern.spark.metrics.monitors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.HashMap;

import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;
import org.junit.Before;
import org.junit.Test;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.status.StatusValue;

public class UpdateMonitorStatusesFTest {
	
	private Cache<Properties> propertiesCache;

	@Before
	public void reset() throws ConfigurationException {
		Properties.initCache(null);
		propertiesCache = Properties.getCache();
		Monitors.getCache().reset();
	}
	
    @Test
    public void timingOutMetric() throws Exception{
    		propertiesCache.get().setProperty("monitor.ID.analysis.type", "fixed-threshold");
        
        UpdateMonitorStatusesF func = new UpdateMonitorStatusesF(null);
        
        Time time = new Time(1000);
        MonitorStatusKey ids = new MonitorStatusKey("ID", new HashMap<String, String>());
        Optional<Metric> metricOpt = null;
        @SuppressWarnings("unchecked")
        State<StatusValue> storeState = mock(State.class);
        when(storeState.isTimingOut()).thenReturn(true);
        
        Optional<AnalysisResult> resultOpt = func.call(time, ids, metricOpt, storeState);
        
        assertTrue(resultOpt.isPresent());
        assertEquals(AnalysisResult.Status.EXCEPTION, resultOpt.get().getStatus());
        assertEquals("Metric has timmed out.", resultOpt.get().getStatusReason());
    }
    
    @Test
    public void noMetric() throws Exception{
        UpdateMonitorStatusesF func = new UpdateMonitorStatusesF(null);
        
        Time time = new Time(1000);
        MonitorStatusKey ids = new MonitorStatusKey("ID", new HashMap<String, String>());
        Optional<Metric> metricOpt = Optional.absent();
        @SuppressWarnings("unchecked")
        State<StatusValue> storeState = mock(State.class);
        
        Optional<AnalysisResult> resultOpt = func.call(time, ids, metricOpt, storeState);
        
        assertFalse(resultOpt.isPresent());
    }
    
}
