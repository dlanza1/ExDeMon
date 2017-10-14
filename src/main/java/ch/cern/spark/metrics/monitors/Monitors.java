package ch.cern.spark.metrics.monitors;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.spark.Cache;
import ch.cern.spark.Pair;
import ch.cern.spark.Properties;
import ch.cern.spark.Properties.PropertiesCache;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.MetricStatusesS;
import ch.cern.spark.metrics.MetricsS;
import ch.cern.spark.metrics.MonitorIDMetricIDs;
import ch.cern.spark.metrics.notifications.NotificationStatusesS;
import ch.cern.spark.metrics.notifications.NotificationStoresRDD;
import ch.cern.spark.metrics.notifications.NotificationsS;
import ch.cern.spark.metrics.notifications.NotificationsWithIdS;
import ch.cern.spark.metrics.notifications.UpdateNotificationStatusesF;
import ch.cern.spark.metrics.notificator.NotificatorID;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResultsS;
import ch.cern.spark.metrics.store.MetricStoresRDD;
import ch.cern.spark.metrics.store.MetricStoresS;
import ch.cern.spark.metrics.store.UpdateMetricStatusesF;

public class Monitors extends Cache<Map<String, Monitor>> implements Serializable{
	private static final long serialVersionUID = 2628296754660438034L;

	private transient final static Logger LOG = Logger.getLogger(Monitors.class.getName());
	
	private PropertiesCache propertiesCache;
	
	private transient JavaSparkContext sparkContext;
	
	private String checkpointDir;

	private Duration dataExpirationPeriod;
	
	public Monitors(PropertiesCache propertiesCache, JavaSparkContext sparkContext, String checkpointDir, Duration dataExpirationPeriod) {
		this.propertiesCache = propertiesCache;
		this.sparkContext = sparkContext;
		this.checkpointDir = checkpointDir;
		this.dataExpirationPeriod = dataExpirationPeriod;
	}

	@Override
	protected Map<String, Monitor> load() throws IOException {
        Properties properties = propertiesCache.get().getSubset("monitor");
        
        Set<String> monitorNames = properties.getUniqueKeyFields();
        
        Map<String, Monitor> monitors = monitorNames.stream()
        		.map(id -> new Pair<String, Properties>(id, properties.getSubset(id)))
        		.map(info -> new Monitor(info.first).config(info.second))
        		.collect(Collectors.toMap(Monitor::getId, m -> m));
        
        LOG.info("Loaded Monitors: " + monitors);
        
        return monitors;
	}
	
	public Collection<Monitor> values() throws IOException {
		return get().values();
	}

	public Monitor get(String monitorID) throws IOException {
		return get().get(monitorID);
	}
	
	public AnalysisResultsS analyze(MetricsS metrics) throws IOException, ClassNotFoundException {
        JavaPairDStream<MonitorIDMetricIDs, Metric> metricsWithID = metrics.withID(this);
        
        MetricStoresRDD initialMetricStores = MetricStoresRDD.load(checkpointDir, sparkContext);
        
        MetricStatusesS statuses = UpdateMetricStatusesF.apply(metricsWithID, this, initialMetricStores, dataExpirationPeriod);
        
        MetricStoresS metricStores = statuses.getMetricStoresStatuses();
        metricStores.save(checkpointDir);
        
        AnalysisResultsS missingMetricsResults = metricStores.missingMetricResults(this);
        
        return statuses.getAnalysisResultsStream().union(missingMetricsResults);
	}

	public NotificationsS notify(AnalysisResultsS results) throws IOException, ClassNotFoundException {
        JavaPairDStream<NotificatorID, AnalysisResult> analysisWithID = results.withNotificatorID(this);
        
        NotificationStoresRDD initialNotificationStores = NotificationStoresRDD.load(checkpointDir, sparkContext);
        
        NotificationStatusesS statuses = UpdateNotificationStatusesF.apply(analysisWithID, this, initialNotificationStores, dataExpirationPeriod);
        
        NotificationsWithIdS allNotificationsStatuses = statuses.getAllNotificationsStatusesWithID();
        allNotificationsStatuses.save(checkpointDir);
        
        return statuses.getThrownNotifications();
	}
	
}
