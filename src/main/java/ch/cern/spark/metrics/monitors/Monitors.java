package ch.cern.spark.metrics.monitors;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.spark.Cache;
import ch.cern.spark.Pair;
import ch.cern.spark.Properties;
import ch.cern.spark.Properties.PropertiesCache;
import ch.cern.spark.RDDHelper;
import ch.cern.spark.metrics.ComputeMissingMetricResultsF;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.MetricStatusesS;
import ch.cern.spark.metrics.MonitorIDMetricIDs;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notifications.NotificationStatusesS;
import ch.cern.spark.metrics.notifications.NotificationStoresRDD;
import ch.cern.spark.metrics.notifications.UpdateNotificationStatusesF;
import ch.cern.spark.metrics.notificator.NotificatorID;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.store.MetricStore;
import ch.cern.spark.metrics.store.MetricStoresRDD;
import ch.cern.spark.metrics.store.Store;
import ch.cern.spark.metrics.store.UpdateMetricStatusesF;
import scala.Tuple2;

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
	
	public JavaDStream<AnalysisResult> analyze(JavaDStream<Metric> metrics) throws IOException, ClassNotFoundException {
        
		JavaRDD<Tuple2<MonitorIDMetricIDs, MetricStore>> initialMetricStores = RDDHelper.<Tuple2<MonitorIDMetricIDs, MetricStore>>load(checkpointDir, sparkContext);
        
        MetricStatusesS statuses = UpdateMetricStatusesF.apply(metrics, this, initialMetricStores, dataExpirationPeriod);
        
        JavaDStream<Tuple2<MonitorIDMetricIDs, MetricStore>> metricStores = statuses.getMetricStoresStatuses();
        metricStores.foreachRDD(rdd -> new MetricStoresRDD(rdd).save(checkpointDir));
        
        JavaDStream<AnalysisResult> missingMetricsResults = metricStores.transform((rdd, time) -> rdd.flatMap(
																		new ComputeMissingMetricResultsF(this, time))
																	);
        
        return statuses.getAnalysisResultsStream().union(missingMetricsResults);
	}

	public JavaDStream<Notification> notify(JavaDStream<AnalysisResult> results) throws IOException, ClassNotFoundException {
        
        JavaRDD<Tuple2<NotificatorID, Store>> initialNotificationStores = RDDHelper.<Tuple2<NotificatorID, Store>>load(checkpointDir, sparkContext);
        
        NotificationStatusesS statuses = UpdateNotificationStatusesF.apply(results, this, initialNotificationStores, dataExpirationPeriod);
        
        JavaDStream<Tuple2<NotificatorID, Store>> allNotificationsStatuses = statuses.getAllNotificationsStatusesWithID();
        allNotificationsStatuses.foreachRDD(rdd -> new NotificationStoresRDD(rdd).save(checkpointDir));
        
        return statuses.getThrownNotifications();
	}
	
}
