package ch.cern.spark.metrics.notifications;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function4;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.spark.Properties;
import ch.cern.spark.Properties.Expirable;
import ch.cern.spark.metrics.monitor.Monitor;
import ch.cern.spark.metrics.notificator.Notificator;
import ch.cern.spark.metrics.notificator.NotificatorID;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.store.HasStore;
import ch.cern.spark.metrics.store.Store;

public class UpdateNotificationStatusesF
        implements Function4<Time, NotificatorID, Optional<AnalysisResult>, State<Store>, Optional<Notification>> {

    private static final long serialVersionUID = 1540971922358997509L;
    
    public static String DATA_EXPIRATION_PARAM = "data.expiration";
    public static java.time.Duration DATA_EXPIRATION_DEFAULT = java.time.Duration.ofHours(3);

    private Map<String, Monitor> monitors = null;

    private Properties.Expirable propertiesExp;

    public UpdateNotificationStatusesF(Properties.Expirable propertiesExp) {
        this.propertiesExp = propertiesExp;
    }

    @Override
    public Optional<Notification> call(Time time, NotificatorID ids, Optional<AnalysisResult> resuktOpt,
            State<Store> notificatorState) throws Exception {

        if (notificatorState.isTimingOut() || !resuktOpt.isPresent())
            return Optional.absent();
        
        Store store = getStore(notificatorState);
        
        Notificator notificator = getMonitor(ids.getMonitorID()).getNotificator(ids.getNotificatorID(), store);        
        
        java.util.Optional<Notification> notification = notificator.process(
											                resuktOpt.get().getStatus(),
											                resuktOpt.get().getAnalyzedMetric().getInstant());
        notification.ifPresent(c -> c.setMetricIDs(new HashMap<>()));
        
        if(notificator instanceof HasStore)
            notificatorState.update(((HasStore) notificator).save());
        
        notification.ifPresent(n -> {
            n.setMonitorID(ids.getMonitorID());
            n.setNotificatorID(ids.getNotificatorID());
            n.setMetricIDs(ids.getMetricIDs());
            n.setTimestamp(resuktOpt.get().getAnalyzedMetric().getInstant());
        });

        return notification.isPresent() ? Optional.of(notification.get()) : Optional.empty();
    }

    private Store getStore(State<Store> notificatorState) {
        return notificatorState.exists() ? notificatorState.get() : null;
    }

    private Monitor getMonitor(String monitorID) throws IOException {
        if (monitors == null)
            monitors = Monitor.getAll(propertiesExp);

        return monitors.get(monitorID);
    }

    public static NotificationStatusesS apply(JavaPairDStream<NotificatorID, AnalysisResult> resultsWithId,
            Expirable propertiesExp, NotificationStoresRDD initialNotificationStores) throws IOException {

        java.time.Duration dataExpirationPeriod = propertiesExp.get().getPeriod(DATA_EXPIRATION_PARAM, DATA_EXPIRATION_DEFAULT).get();

        StateSpec<NotificatorID, AnalysisResult, Store, Notification> statusSpec = StateSpec
                .function(new UpdateNotificationStatusesF(propertiesExp)).initialState(initialNotificationStores.rdd())
                .timeout(new Duration(dataExpirationPeriod.toMillis()));

        NotificationStatusesS statuses = new NotificationStatusesS(resultsWithId.mapWithState(statusSpec));

        return statuses;
    }

}
