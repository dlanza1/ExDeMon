package ch.cern.spark.metrics.notificator;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;

import org.apache.spark.streaming.State;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.monitors.Monitor;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.status.HasStatus;
import ch.cern.spark.status.UpdateStatusFunction;

public class UpdateNotificatorStatusesF
        extends UpdateStatusFunction<NotificatorStatusKey, AnalysisResult, NotificatorStatus, Notification> {

    private static final long serialVersionUID = 1540971922358997509L;

    private Properties propertiesSourceProperties;

    public UpdateNotificatorStatusesF(Properties propertiesSourceProps) {
        this.propertiesSourceProperties = propertiesSourceProps;
    }

    @Override
    protected Optional<Notification> update(NotificatorStatusKey ids, AnalysisResult result, State<NotificatorStatus> state) throws Exception {
        Optional<Monitor> monitorOpt = getMonitor(ids.getMonitor_id());
        if (!monitorOpt.isPresent())
            return Optional.empty();
        Monitor monitor = monitorOpt.get();
        
        Notificator notificator = monitor.getNotificators().get(ids.getNotificatorID());
        if (notificator == null)
            return Optional.empty();
        
        NotificatorStatus status = getStatus(state);
        
        Optional<Notification> notification = Optional.empty();
        
        if(!isSilentPeriod(status, notificator.getSilentPeriod(), result.getAnalyzedMetric().getTimestamp())) {
            if (notificator.hasStatus())
                ((HasStatus) notificator).load(status.getActiveStatus());

            notification = notificator.apply(result);
            
            if(notification.isPresent())
                status.setLastRaised(notification.get().getNotification_timestamp());

            if (notificator.hasStatus())
                status.setActiveStatus(((HasStatus) notificator).save());
            
            state.update(status);
        }else {
            Notificator silentPeriodNotificator = notificator.getSilentPeriodNotificator();
            if(silentPeriodNotificator == null)
                return Optional.empty();
            
            if (silentPeriodNotificator.hasStatus())
                ((HasStatus) silentPeriodNotificator).load(status.getSailentStatus());

            notification = silentPeriodNotificator.apply(result);
            
            if(notification.isPresent())
                status.setLastRaised(Instant.EPOCH);

            if (silentPeriodNotificator.hasStatus()) {
                if(notification.isPresent())
                    status.setSailentStatus(null);
                else
                    status.setSailentStatus(((HasStatus) silentPeriodNotificator).save());
            }
            
            state.update(status);
        }

        notification.ifPresent(n -> {
            n.setMonitor_id(ids.getMonitor_id());
            n.setNotificator_id(ids.getNotificatorID());
            n.setMetric_attributes(ids.getMetric_attributes());
            n.setNotification_timestamp(result.getAnalyzedMetric().getTimestamp());
        });

        return notification;
    }

    private boolean isSilentPeriod(NotificatorStatus status, Duration silentPeriod, Instant time) {
        return status.getLastRaised().plus(silentPeriod).isAfter(time);
    }

    private NotificatorStatus getStatus(State<NotificatorStatus> state) {
        return state.exists() ? state.get() : new NotificatorStatus();
    }

    protected Optional<Monitor> getMonitor(String monitor_id) throws Exception {
        Monitors.initCache(propertiesSourceProperties);
        
        return Optional.ofNullable(Monitors.getCache().get().get(monitor_id));
    }

}
