package ch.cern.spark.metrics.monitors;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import ch.cern.Cache;
import ch.cern.Properties;
import ch.cern.Properties.PropertiesCache;
import ch.cern.spark.StatusStream;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.ComputeIDsForMetricsF;
import ch.cern.spark.metrics.ComputeMissingMetricResultsF;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.MonitorIDMetricIDs;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notifications.UpdateNotificationStatusesF;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.ComputeIDsForAnalysisF;
import ch.cern.spark.metrics.store.MetricStore;
import ch.cern.spark.metrics.store.UpdateMetricStatusesF;

public class Monitors extends Cache<Map<String, Monitor>> implements Serializable{
	private static final long serialVersionUID = 2628296754660438034L;

	private transient final static Logger LOG = Logger.getLogger(Monitors.class.getName());
	
	private PropertiesCache propertiesCache;
	
	public Monitors(PropertiesCache propertiesCache) {
		this.propertiesCache = propertiesCache;
	}

	@Override
	protected Map<String, Monitor> load() throws Exception {
        Properties properties = propertiesCache.get().getSubset("monitor");
        
        Set<String> monitorNames = properties.getUniqueKeyFields();
        
        Map<String, Monitor> monitors = new HashMap<>();
        for (String monitorName : monitorNames) {
			Properties monitorProps = properties.getSubset(monitorName);
			
			monitors.put(monitorName, new Monitor(monitorName).config(monitorProps));
		}

        LOG.info("Loaded Monitors: " + monitors);
        
        return monitors;
	}
	
	public Collection<Monitor> values() throws Exception {
		return get().values();
	}

	public Monitor get(String monitorID) throws Exception {
		return get().get(monitorID);
	}
	
	public Stream<AnalysisResult> analyze(Stream<Metric> metrics) throws Exception {
		StatusStream<MonitorIDMetricIDs, Metric, MetricStore, AnalysisResult> statuses = metrics.mapWithState("metricStores", new ComputeIDsForMetricsF(this), new UpdateMetricStatusesF(this));
        
        Stream<AnalysisResult> missingMetricsResults = statuses.getStatuses().transform((rdd, time) -> rdd.flatMap(new ComputeMissingMetricResultsF(this, time)));
        
        return statuses.union(missingMetricsResults);
	}

	public Stream<Notification> notify(Stream<AnalysisResult> results) throws IOException, ClassNotFoundException {
        return results.mapWithState("notificators", new ComputeIDsForAnalysisF(this), new UpdateNotificationStatusesF(this));
	}
	
}
