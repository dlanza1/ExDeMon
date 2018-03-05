package ch.cern.spark.metrics.trigger;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.apache.spark.streaming.State;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.monitors.Monitor;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.trigger.action.Action;
import ch.cern.spark.status.HasStatus;
import ch.cern.spark.status.UpdateStatusFunction;

public class UpdateTriggerStatusesF
        extends UpdateStatusFunction<TriggerStatusKey, AnalysisResult, TriggerStatus, Action> {

    private static final long serialVersionUID = 1540971922358997509L;

    private Properties propertiesSourceProperties;

    public UpdateTriggerStatusesF(Properties propertiesSourceProps) {
        this.propertiesSourceProperties = propertiesSourceProps;
    }

    @Override
    protected Optional<Action> update(TriggerStatusKey ids, AnalysisResult result, State<TriggerStatus> state) throws Exception {
        Optional<Monitor> monitorOpt = getMonitor(ids.getMonitor_id());
        if (!monitorOpt.isPresent())
            return Optional.empty();
        Monitor monitor = monitorOpt.get();
        
        Trigger trigger = monitor.getTriggers().get(ids.getNotificatorID());
        if (trigger == null)
            return Optional.empty();
        
        TriggerStatus status = getStatus(state);
        
        Optional<Action> actionOpt = Optional.empty();
        
        if(!isSilentPeriod(status, trigger.getSilentPeriod(), result.getAnalyzed_metric().getTimestamp())) {
            if (trigger.hasStatus())
                ((HasStatus) trigger).load(status.getActiveStatus());

            actionOpt = trigger.apply(result);
            
            if(actionOpt.isPresent())
                status.setLastRaised(actionOpt.get().getTriggeringResult().getAnalyzed_metric().getTimestamp());

            if (trigger.hasStatus())
                status.setActiveStatus(((HasStatus) trigger).save());
            
            state.update(status);
        }else {
            Trigger silentPeriodTrigger = trigger.getSilentPeriodTrigger();
            if(silentPeriodTrigger == null)
                return Optional.empty();
            
            if (silentPeriodTrigger.hasStatus())
                ((HasStatus) silentPeriodTrigger).load(status.getSailentStatus());

            actionOpt = silentPeriodTrigger.apply(result);
            
            if(actionOpt.isPresent())
                status.setLastRaised(Instant.EPOCH);

            if (silentPeriodTrigger.hasStatus()) {
                if(actionOpt.isPresent())
                    status.setSailentStatus(null);
                else
                    status.setSailentStatus(((HasStatus) silentPeriodTrigger).save());
            }
            
            state.update(status);
        }

        actionOpt.ifPresent(a -> {
            a.setMonitor_id(ids.getMonitor_id());
            a.setTrigger_id(ids.getNotificatorID());
            a.setMetric_attributes(ids.getMetric_attributes());
            a.setCreation_timestamp(result.getAnalyzed_metric().getTimestamp());
        });

        return actionOpt;
    }

    private boolean isSilentPeriod(TriggerStatus status, Duration silentPeriod, Instant time) {
        return status.getLastRaised().plus(silentPeriod).isAfter(time);
    }

    private TriggerStatus getStatus(State<TriggerStatus> state) {
        return state.exists() ? state.get() : new TriggerStatus();
    }

    protected Optional<Monitor> getMonitor(String monitor_id) throws Exception {
        Monitors.initCache(propertiesSourceProperties);
        
        return Optional.ofNullable(Monitors.getCache().get().get(monitor_id));
    }

}
