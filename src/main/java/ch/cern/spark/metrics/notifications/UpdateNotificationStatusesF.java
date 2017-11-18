package ch.cern.spark.metrics.notifications;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.Time;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.monitors.Monitor;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.notificator.Notificator;
import ch.cern.spark.metrics.notificator.NotificatorID;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.store.Store;

public class UpdateNotificationStatusesF
        implements Function4<Time, NotificatorID, Optional<AnalysisResult>, State<Store>, Optional<Notification>> {

    private static final long serialVersionUID = 1540971922358997509L;
    
    private Properties propertiesSourceProperties;

    public UpdateNotificationStatusesF(Properties propertiesSourceProps) {
    		this.propertiesSourceProperties = propertiesSourceProps;
    }

    @Override
    public Optional<Notification> call(Time time, NotificatorID ids, Optional<AnalysisResult> resultOpt,
            State<Store> notificatorState) throws Exception {
    		Monitors.initCache(propertiesSourceProperties);

        if (notificatorState.isTimingOut() || !resultOpt.isPresent())
            return Optional.absent();
        
        Optional<Monitor> monitorOpt = Optional.fromNullable(Monitors.getCache().get().get(ids.getMonitorID()));
        if(!monitorOpt.isPresent())
        		return Optional.empty();
        Monitor monitor = monitorOpt.get();
        
        Notificator notificator = monitor.getNotificators().get(ids.getNotificatorID());        
        if(notificator.hasStore())
        		toOptional(notificatorState).ifPresent(((HasStore) notificator)::load);
        
        java.util.Optional<Notification> notification = notificator.apply(resultOpt.get());
        
        notificator.getStore().ifPresent(notificatorState::update);
        
        notification.ifPresent(n -> {
            n.setMonitorID(ids.getMonitorID());
            n.setNotificatorID(ids.getNotificatorID());
            n.setMetricIDs(ids.getMetricIDs());
            n.setTimestamp(resultOpt.get().getAnalyzedMetric().getInstant());
        });

        return notification.isPresent() ? Optional.of(notification.get()) : Optional.empty();
    }

    private java.util.Optional<Store> toOptional(State<Store> notificatorState) {
        return notificatorState.exists() ? 
        				java.util.Optional.of(notificatorState.get()) 
        				: java.util.Optional.empty();
    }

}
