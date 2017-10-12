package ch.cern.spark.metrics.store;

import java.io.IOException;
import java.time.Instant;
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
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.MetricStatusesS;
import ch.cern.spark.metrics.MonitorIDMetricIDs;
import ch.cern.spark.metrics.monitor.Monitor;
import ch.cern.spark.metrics.results.AnalysisResult;

public class UpdateMetricStatusesF
        implements Function4<Time, MonitorIDMetricIDs, Optional<Metric>, State<MetricStore>, Optional<AnalysisResult>> {

    private static final long serialVersionUID = 3156649511706333348L;
    
    public static String DATA_EXPIRATION_PARAM = "data.expiration";
    public static java.time.Duration DATA_EXPIRATION_DEFAULT = java.time.Duration.ofHours(3);

    private Map<String, Monitor> monitors = null;

    private Properties.Expirable propertiesExp;
    
    public UpdateMetricStatusesF(Properties.Expirable propertiesExp) {
        this.propertiesExp = propertiesExp;
    }

    @Override
    public Optional<AnalysisResult> call(
            Time time, MonitorIDMetricIDs ids, Optional<Metric> metricOpt, State<MetricStore> storeState) 
            throws Exception {
        
        Monitor monitor = getMonitor(ids.getMonitorID());
        
        if(storeState.isTimingOut())
            return Optional.of(AnalysisResult.buildTimingOut(ids, monitor, Instant.ofEpochMilli(time.milliseconds())));
        
        if(!metricOpt.isPresent())
            return Optional.absent();
        
        MetricStore store = getMetricStore(storeState);
        Metric metric = metricOpt.get();
        
        store.updateLastestTimestamp(metric.getInstant());
        
        AnalysisResult result = monitor.process(store, metric);
        result.setAnalyzedMetric(metric);
        
        storeState.update(store);
        
        return Optional.of(result);
    }

    private Monitor getMonitor(String monitorID) throws IOException {
        if(monitors == null)
            monitors = Monitor.getAll(propertiesExp);
        
        return monitors.get(monitorID);
    }
    
    private MetricStore getMetricStore(State<MetricStore> storeState) {
        return storeState.exists() ? storeState.get() : new MetricStore();
    }

    public static MetricStatusesS apply(
            JavaPairDStream<MonitorIDMetricIDs, Metric> metricsWithID,
            Expirable propertiesExp, 
            MetricStoresRDD initialMetricStores) throws IOException {
        
    		java.time.Duration dataExpirationPeriod = propertiesExp.get().getPeriod(DATA_EXPIRATION_PARAM, DATA_EXPIRATION_DEFAULT).get();
        
        StateSpec<MonitorIDMetricIDs, Metric, MetricStore, AnalysisResult> statusSpec = StateSpec
                .function(new UpdateMetricStatusesF(propertiesExp))
                .initialState(initialMetricStores.rdd())
                .timeout(new Duration(dataExpirationPeriod.toMillis()));
        
        MetricStatusesS statuses = new MetricStatusesS(metricsWithID.mapWithState(statusSpec));
        
        return statuses;
    }

}
