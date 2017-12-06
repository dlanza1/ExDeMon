package ch.cern.spark.metrics.notificator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;

import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateImpl;
import org.apache.spark.streaming.Time;
import org.junit.Before;
import org.junit.Test;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notificator.NotificatorStatusKey;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.status.StatusValue;

public class UpdateNotificatorStatusesFTest {
	
	private Cache<Properties> propertiesCache;
	
	@Before
	public void reset() throws ConfigurationException {
		Properties.initCache(null);
		propertiesCache = Properties.getCache();
		propertiesCache.reset();
		Monitors.getCache().reset();
	}

    @Test
    public void raiseAlwaysSameStatus() throws Exception{
    		propertiesCache.get().setProperty("monitor.monID.analysis.type", "true");
    		propertiesCache.get().setProperty("monitor.monID.notificator.notID.type", "constant");
    		propertiesCache.get().setProperty("monitor.monID.notificator.notID.statuses", "error");
    		propertiesCache.get().setProperty("monitor.monID.notificator.notID.period", "10s");
        
        UpdateNotificatorStatusesF func = new UpdateNotificatorStatusesF(null);
        
        Optional<Notification> notification = null;
                
        NotificatorStatusKey ids = new NotificatorStatusKey("monID", "notID", null);
        Optional<AnalysisResult> resuktOpt = Optional.of(new AnalysisResult());
        resuktOpt.get().setStatus(Status.ERROR, "");
        
        State<StatusValue> storeState = new StateImpl<StatusValue>();
        
        resuktOpt.get().setAnalyzedMetric(new Metric(Instant.ofEpochSecond(1), 0f, null));
        notification = func.call(new Time(0), ids, resuktOpt, storeState);
        assertFalse(notification.isPresent());
        
        resuktOpt.get().setAnalyzedMetric(new Metric(Instant.ofEpochSecond(21), 0f, null));
        notification = func.call(new Time(0), ids, resuktOpt, storeState);
        assertTrue(notification.isPresent());
    }
    
    @Test
    public void notRaiseAfterRaising() throws Exception{
    		propertiesCache.get().setProperty("monitor.monID.analysis.type", "true");
    		propertiesCache.get().setProperty("monitor.monID.notificator.notID.type", "constant");
    		propertiesCache.get().setProperty("monitor.monID.notificator.notID.statuses", "error");
    		propertiesCache.get().setProperty("monitor.monID.notificator.notID.period", "10s");
    		
        UpdateNotificatorStatusesF func = new UpdateNotificatorStatusesF(null);
        
        Optional<Notification> notification = null;
                
        NotificatorStatusKey ids = new NotificatorStatusKey("monID", "notID", null);
        Optional<AnalysisResult> resuktOpt = Optional.of(new AnalysisResult());
        resuktOpt.get().setStatus(Status.ERROR, "");
        
        State<StatusValue> storeState = new StateImpl<StatusValue>();
        
        resuktOpt.get().setAnalyzedMetric(new Metric(Instant.ofEpochSecond(10), 0f, null));
        notification = func.call(new Time(0), ids, resuktOpt, storeState);
        assertFalse(notification.isPresent());
        
        resuktOpt.get().setAnalyzedMetric(new Metric(Instant.ofEpochSecond(21), 0f, null));
        notification = func.call(new Time(0), ids, resuktOpt, storeState);
        assertTrue(notification.isPresent());
        
        resuktOpt.get().setAnalyzedMetric(new Metric(Instant.ofEpochSecond(27), 0f, null));
        notification = func.call(new Time(0), ids, resuktOpt, storeState);
        assertFalse(notification.isPresent());
    }
    
    @Test
    public void raiseChangingStatus() throws Exception{
    		propertiesCache.get().setProperty("monitor.monID.analysis.type", "true");
    		propertiesCache.get().setProperty("monitor.monID.notificator.notID.type", "constant");
    		propertiesCache.get().setProperty("monitor.monID.notificator.notID.statuses", "error");
    		propertiesCache.get().setProperty("monitor.monID.notificator.notID.period", "10s");
        
        UpdateNotificatorStatusesF func = new UpdateNotificatorStatusesF(null);
        
        Optional<Notification> notification = null;
                
        NotificatorStatusKey ids = new NotificatorStatusKey("monID", "notID", null);
        Optional<AnalysisResult> resuktOpt = Optional.of(new AnalysisResult());
        
        State<StatusValue> storeState = new StateImpl<StatusValue>();
        
        resuktOpt.get().setStatus(Status.ERROR, "");
        resuktOpt.get().setAnalyzedMetric(new Metric(Instant.ofEpochSecond(10), 0f, null));
        notification = func.call(new Time(0), ids, resuktOpt, storeState);
        assertFalse(notification.isPresent());
        
        resuktOpt.get().setStatus(Status.WARNING, "");
        resuktOpt.get().setAnalyzedMetric(new Metric(Instant.ofEpochSecond(21), 0f, null));
        notification = func.call(new Time(0), ids, resuktOpt, storeState);
        assertFalse(notification.isPresent());
        
        resuktOpt.get().setStatus(Status.ERROR, "");
        resuktOpt.get().setAnalyzedMetric(new Metric(Instant.ofEpochSecond(31), 0f, null));
        notification = func.call(new Time(0), ids, resuktOpt, storeState);
        assertFalse(notification.isPresent());
        
        resuktOpt.get().setStatus(Status.ERROR, "");
        resuktOpt.get().setAnalyzedMetric(new Metric(Instant.ofEpochSecond(35), 0f, null));
        notification = func.call(new Time(0), ids, resuktOpt, storeState);
        assertFalse(notification.isPresent());
        
        resuktOpt.get().setStatus(Status.ERROR, "");
        resuktOpt.get().setAnalyzedMetric(new Metric(Instant.ofEpochSecond(42), 0f, null));
        notification = func.call(new Time(0), ids, resuktOpt, storeState);
        assertTrue(notification.isPresent());
    }
    
}
