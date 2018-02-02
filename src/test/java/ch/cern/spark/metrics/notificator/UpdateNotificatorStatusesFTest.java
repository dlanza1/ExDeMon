package ch.cern.spark.metrics.notificator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
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
                
        NotificatorStatusKey ids = new NotificatorStatusKey("monID", "notID", new HashMap<>());
        AnalysisResult result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
        
        State<NotificatorStatus> storeState = new StateImpl<NotificatorStatus>();
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(1), 0f, null));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(21), 0f, null));
        notification = func.update(ids, result, storeState);
        assertTrue(notification.isPresent());
    }
    
    @Test
    public void silentPeriodDefault() throws Exception{
    		Properties monProps = new Properties();
        monProps.setProperty("analysis.type", "true");
    		monProps.setProperty("notificator.notID.type", "statuses");
    		monProps.setProperty("notificator.notID.statuses", "error");
    		Monitor monitor = new Monitor("monID");
        monitor.config(monProps);
            
        UpdateNotificatorStatusesF func = new UpdateNotificatorStatusesFWithMonitor(monitor);
        
        Optional<Notification> notification = null;
                
        NotificatorStatusKey ids = new NotificatorStatusKey("monID", "notID", new HashMap<>());
        AnalysisResult result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
        
        State<NotificatorStatus> storeState = new StateImpl<NotificatorStatus>();
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(10), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertTrue(notification.isPresent());
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(21), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertTrue(notification.isPresent());
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(27), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertTrue(notification.isPresent());
    }
    
    @Test
    public void silentPeriodWithoutSilentNotificatior() throws Exception{
        Properties monProps = new Properties();
        monProps.setProperty("analysis.type", "true");
        monProps.setProperty("notificator.notID.type", "statuses");
        monProps.setProperty("notificator.notID.statuses", "error");
        monProps.setProperty("notificator.notID.silent.period", "1m");
        Monitor monitor = new Monitor("monID");
        monitor.config(monProps);
            
        UpdateNotificatorStatusesF func = new UpdateNotificatorStatusesFWithMonitor(monitor);
        
        Optional<Notification> notification = null;
                
        NotificatorStatusKey ids = new NotificatorStatusKey("monID", "notID", new HashMap<>());
        AnalysisResult result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
        
        State<NotificatorStatus> storeState = new StateImpl<NotificatorStatus>();
        
        Instant now = Instant.now();
        
        result.setAnalyzedMetric(new Metric(now, 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertTrue(notification.isPresent());
        
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(10)), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(50)), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(60)), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertTrue(notification.isPresent());
    }
    
    @Test
    public void silentPeriodWithSilentNotificatior() throws Exception{
        Properties monProps = new Properties();
        monProps.setProperty("analysis.type", "true");
        monProps.setProperty("notificator.notID.type", "statuses");
        monProps.setProperty("notificator.notID.statuses", "error");
        monProps.setProperty("notificator.notID.silent.period", "1m");
        monProps.setProperty("notificator.notID.silent.notificator.type", "statuses");
        monProps.setProperty("notificator.notID.silent.notificator.statuses", "ok");
        Monitor monitor = new Monitor("monID");
        monitor.config(monProps);
            
        UpdateNotificatorStatusesF func = new UpdateNotificatorStatusesFWithMonitor(monitor);
        
        Optional<Notification> notification = null;
                
        NotificatorStatusKey ids = new NotificatorStatusKey("monID", "notID", new HashMap<>());
        AnalysisResult result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
        
        State<NotificatorStatus> storeState = new StateImpl<NotificatorStatus>();
        
        Instant now = Instant.now();
        
        result.setAnalyzedMetric(new Metric(now, 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertTrue(notification.isPresent());
        // silent period for the next 60 seconds
        
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(10)), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(20)), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(30)), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        // silent notificator raises notification
        result.setStatus(Status.OK, "");
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(40)), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertTrue(notification.isPresent());
        // silent period is over
        
        result.setStatus(Status.OK, "");
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(45)), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setStatus(Status.ERROR, "");
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(50)), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertTrue(notification.isPresent());
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
                
        NotificatorStatusKey ids = new NotificatorStatusKey("monID", "notID", new HashMap<>());
        AnalysisResult result = new AnalysisResult();
        
        State<NotificatorStatus> storeState = new StateImpl<NotificatorStatus>();
        
        result.setStatus(Status.ERROR, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(10), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setStatus(Status.WARNING, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(21), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setStatus(Status.ERROR, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(31), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setStatus(Status.ERROR, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(35), 0f, new HashMap<>()));
        notification = func.update(ids, result, storeState);
        assertFalse(notification.isPresent());
        
        result.setStatus(Status.ERROR, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(42), 0f, new HashMap<>()));
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
