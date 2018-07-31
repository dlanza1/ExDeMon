package ch.cern.exdemon.monitor.trigger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Optional;

import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateImpl;
import org.junit.Test;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.Monitor;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.properties.Properties;

public class UpdateTriggerStatusesFTest {

    @Test
    public void raiseAlwaysSameStatus() throws Exception{
        Properties monProps = new Properties();
    		monProps.setProperty("analysis.type", "true");
    		monProps.setProperty("triggers.notID.type", "constant");
    		monProps.setProperty("triggers.notID.statuses", "error");
    		monProps.setProperty("triggers.notID.period", "10s");
    		Monitor monitor = new Monitor("monID");
    		monitor.config(monProps);
    		
        UpdateTriggerStatusesF func = new UpdateNotificatorStatusesFWithMonitor(monitor);
        
        Optional<Action> action = null;
                
        TriggerStatusKey ids = new TriggerStatusKey("monID", "notID", new HashMap<>());
        AnalysisResult result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
        
        State<TriggerStatus> storeState = new StateImpl<TriggerStatus>();
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(1), 0f, null));
        action = func.update(ids, result, storeState);
        assertFalse(action.isPresent());
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(21), 0f, null));
        action = func.update(ids, result, storeState);
        assertTrue(action.isPresent());
    }
    
    @Test
    public void silentPeriodDefault() throws Exception{
    		Properties monProps = new Properties();
        monProps.setProperty("analysis.type", "true");
    		monProps.setProperty("triggers.notID.type", "statuses");
    		monProps.setProperty("triggers.notID.statuses", "error");
    		Monitor monitor = new Monitor("monID");
        monitor.config(monProps);
            
        UpdateTriggerStatusesF func = new UpdateNotificatorStatusesFWithMonitor(monitor);
        
        Optional<Action> action = null;
                
        TriggerStatusKey ids = new TriggerStatusKey("monID", "notID", new HashMap<>());
        AnalysisResult result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
        
        State<TriggerStatus> storeState = new StateImpl<TriggerStatus>();
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(10), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertTrue(action.isPresent());
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(21), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertTrue(action.isPresent());
        
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(27), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertTrue(action.isPresent());
    }
    
    @Test
    public void silentPeriodWithoutSilentNotificatior() throws Exception{
        Properties monProps = new Properties();
        monProps.setProperty("analysis.type", "true");
        monProps.setProperty("triggers.notID.type", "statuses");
        monProps.setProperty("triggers.notID.statuses", "error");
        monProps.setProperty("triggers.notID.silent.period", "1m");
        Monitor monitor = new Monitor("monID");
        monitor.config(monProps);
            
        UpdateTriggerStatusesF func = new UpdateNotificatorStatusesFWithMonitor(monitor);
        
        Optional<Action> action = null;
                
        TriggerStatusKey ids = new TriggerStatusKey("monID", "notID", new HashMap<>());
        AnalysisResult result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
        
        State<TriggerStatus> storeState = new StateImpl<TriggerStatus>();
        
        Instant now = Instant.now();
        
        result.setAnalyzedMetric(new Metric(now, 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertTrue(action.isPresent());
        
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(10)), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertFalse(action.isPresent());
        
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(50)), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertFalse(action.isPresent());
        
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(60)), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertTrue(action.isPresent());
    }
    
    @Test
    public void silentPeriodWithSilentNotificatior() throws Exception{
        Properties monProps = new Properties();
        monProps.setProperty("analysis.type", "true");
        monProps.setProperty("triggers.notID.type", "statuses");
        monProps.setProperty("triggers.notID.statuses", "error");
        monProps.setProperty("triggers.notID.silent.period", "1m");
        monProps.setProperty("triggers.notID.silent.trigger.type", "statuses");
        monProps.setProperty("triggers.notID.silent.trigger.statuses", "ok");
        Monitor monitor = new Monitor("monID");
        monitor.config(monProps);
            
        UpdateTriggerStatusesF func = new UpdateNotificatorStatusesFWithMonitor(monitor);
        
        Optional<Action> action = null;
                
        TriggerStatusKey ids = new TriggerStatusKey("monID", "notID", new HashMap<>());
        AnalysisResult result = new AnalysisResult();
        result.setStatus(Status.ERROR, "");
        
        State<TriggerStatus> storeState = new StateImpl<TriggerStatus>();
        
        Instant now = Instant.now();
        
        result.setAnalyzedMetric(new Metric(now, 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertTrue(action.isPresent());
        // silent period for the next 60 seconds
        
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(10)), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertFalse(action.isPresent());
        
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(20)), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertFalse(action.isPresent());
        
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(30)), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertFalse(action.isPresent());
        
        // silent trigger raises action
        result.setStatus(Status.OK, "");
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(40)), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertTrue(action.isPresent());
        // silent period is over
        
        result.setStatus(Status.OK, "");
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(45)), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertFalse(action.isPresent());
        
        result.setStatus(Status.ERROR, "");
        result.setAnalyzedMetric(new Metric(now.plus(Duration.ofSeconds(50)), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertTrue(action.isPresent());
    }
    
    @Test
    public void raiseChangingStatus() throws Exception{
    		Properties monProps = new Properties();
    		monProps.setProperty("analysis.type", "true");
    		monProps.setProperty("triggers.notID.type", "constant");
    		monProps.setProperty("triggers.notID.statuses", "error");
    		monProps.setProperty("triggers.notID.period", "10s");
    		Monitor monitor = new Monitor("monID");
        monitor.config(monProps);
            
        UpdateTriggerStatusesF func = new UpdateNotificatorStatusesFWithMonitor(monitor);
        
        Optional<Action> action = null;
                
        TriggerStatusKey ids = new TriggerStatusKey("monID", "notID", new HashMap<>());
        AnalysisResult result = new AnalysisResult();
        
        State<TriggerStatus> storeState = new StateImpl<TriggerStatus>();
        
        result.setStatus(Status.ERROR, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(10), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertFalse(action.isPresent());
        
        result.setStatus(Status.WARNING, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(21), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertFalse(action.isPresent());
        
        result.setStatus(Status.ERROR, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(31), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertFalse(action.isPresent());
        
        result.setStatus(Status.ERROR, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(35), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertFalse(action.isPresent());
        
        result.setStatus(Status.ERROR, "");
        result.setAnalyzedMetric(new Metric(Instant.ofEpochSecond(42), 0f, new HashMap<>()));
        action = func.update(ids, result, storeState);
        assertTrue(action.isPresent());
    }
    
    public static class UpdateNotificatorStatusesFWithMonitor extends UpdateTriggerStatusesF{
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
