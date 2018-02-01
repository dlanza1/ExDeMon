package ch.cern.spark.metrics.notificator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.Optional;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateImpl;
import org.junit.Test;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.monitors.Monitor;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.status.StatusValue;

public class UpdateNotificatorStatusesFTest {

    @Test
    public void raiseAlwaysSameStatus() throws Exception{
        Properties monProps = new Properties();
    		monProps.setProperty("analysis.type", "true");
    		monProps.setProperty("notificator.notID.type", "constant");
    		monProps.setProperty("notificator.notID.statuses", "error");
    		monProps.setProperty("notificator.notID.period", "10s");
    		Monitor monitor = new Monitor("monID");
    		monitor.config(monProps);
    		
        UpdateNotificatorStatusesF func = new UpdateNotificatorStatusesFWithMonitor(monitor);
        
        Optional<Notification> notification = null;
                
        NotificatorStatusKey ids = new NotificatorStatusKey("monID", "notID", null);
        AnalysisResult result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
        
        State<StatusValue> storeState = new StateImpl<StatusValue>();
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(1), 0f, null));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(21), 0f, null));
        notification = func.update(ids, result, storeState);
        assertTrue(notification.isPresent());
    }
    
    @Test
    public void notRaiseAfterRaising() throws Exception{
    		Properties monProps = new Properties();
        monProps.setProperty("analysis.type", "true");
    		monProps.setProperty("notificator.notID.type", "constant");
    		monProps.setProperty("notificator.notID.statuses", "error");
    		monProps.setProperty("notificator.notID.period", "10s");
    		Monitor monitor = new Monitor("monID");
        monitor.config(monProps);
            
        UpdateNotificatorStatusesF func = new UpdateNotificatorStatusesFWithMonitor(monitor);
        
        Optional<Notification> notification = null;
                
        NotificatorStatusKey ids = new NotificatorStatusKey("monID", "notID", null);
        AnalysisResult result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
        
        State<StatusValue> storeState = new StateImpl<StatusValue>();
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(10), 0f, null));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(21), 0f, null));
        notification = func.update(ids, result, storeState);
        assertTrue(notification.isPresent());
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(27), 0f, null));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
    }
    
    @Test
    public void raiseChangingStatus() throws Exception{
    		Properties monProps = new Properties();
    		monProps.setProperty("analysis.type", "true");
    		monProps.setProperty("notificator.notID.type", "constant");
    		monProps.setProperty("notificator.notID.statuses", "error");
    		monProps.setProperty("notificator.notID.period", "10s");
    		Monitor monitor = new Monitor("monID");
        monitor.config(monProps);
            
        UpdateNotificatorStatusesF func = new UpdateNotificatorStatusesFWithMonitor(monitor);
        
        Optional<Notification> notification = null;
                
        NotificatorStatusKey ids = new NotificatorStatusKey("monID", "notID", null);
        AnalysisResult result = new AnalysisResult();
        
        State<StatusValue> storeState = new StateImpl<StatusValue>();
        
        result.setStatus(Status.ERROR, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(10), 0f, null));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setStatus(Status.WARNING, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(21), 0f, null));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setStatus(Status.ERROR, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(31), 0f, null));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setStatus(Status.ERROR, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(35), 0f, null));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setStatus(Status.ERROR, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(42), 0f, null));
        notification = func.update(ids, result, storeState);
        assertTrue(notification.isPresent());
    }
    
    public static class UpdateNotificatorStatusesFWithMonitor extends UpdateNotificatorStatusesF{
        private static final long serialVersionUID = 1L;
        
        private Monitor monitor;
        
        public UpdateNotificatorStatusesFWithMonitor(Monitor monitor) {
            super(null);
            
            this.monitor = monitor;
        }
        
        @Override
        protected Optional<Monitor> getMonitor(String monitor_id) throws Exception {
            return Optional.ofNullable(monitor);
        }

    }
    
}
