package ch.cern.exdemon.monitor.trigger;

import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.Optional;

import org.apache.spark.streaming.State;

import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.monitor.Monitor;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.exdemon.monitor.trigger.action.template.Template;
import ch.cern.properties.Properties;
import ch.cern.spark.status.HasStatus;
import ch.cern.spark.status.UpdateStatusFunction;

public class UpdateTriggerStatusesF
        extends UpdateStatusFunction<TriggerStatusKey, AnalysisResult, TriggerStatus, Action> {

    private static final long serialVersionUID = 1540971922358997509L;

    private Properties componentsSourceProperties;

    public UpdateTriggerStatusesF(Properties componentsSourceProperties) {
        this.componentsSourceProperties = componentsSourceProperties;
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
            
            //Apply template to tags
            Map<String, String> tags = a.getTags();
            for (Map.Entry<String, String> tag : tags.entrySet())
                tags.put(tag.getKey(), Template.apply(tag.getValue(), a));
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
        ComponentsCatalog.init(componentsSourceProperties);
        
        return ComponentsCatalog.get(Type.MONITOR, monitor_id);
    }

}
