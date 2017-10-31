package ch.cern.spark.metrics.monitors;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import ch.cern.Cache;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.ComputeIDsForMetricsF;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.UpdateMetricStatusesF;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notifications.UpdateNotificationStatusesF;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.ComputeIDsForAnalysisF;

public class Monitors {

	private transient final static Logger LOG = Logger.getLogger(Monitors.class.getName());
	
	private static Cache<Map<String, Monitor>> cachedMonitors = new Cache<Map<String,Monitor>>() {
		
		@Override
		protected Map<String, Monitor> load() throws Exception {
	        Properties properties = Properties.getCache().get().getSubset("monitor");
	        
	        Set<String> monitorNames = properties.getUniqueKeyFields();
	        
	        Map<String, Monitor> monitors = new HashMap<>();
	        for (String monitorName : monitorNames) {
				Properties monitorProps = properties.getSubset(monitorName);
				
				monitors.put(monitorName, new Monitor(monitorName).config(monitorProps));
			}

	        LOG.info("Loaded Monitors: " + monitors);
	        
	        return monitors;
		}
	};
	
	public static Stream<AnalysisResult> analyze(Stream<Metric> metrics, Properties propertiesSourceProps) throws Exception {
        return metrics.mapWithState("metricStores", new ComputeIDsForMetricsF(propertiesSourceProps), new UpdateMetricStatusesF(propertiesSourceProps));
	}

	public static Stream<Notification> notify(Stream<AnalysisResult> results, Properties propertiesSourceProps) throws IOException, ClassNotFoundException, ConfigurationException {
        return results.mapWithState("notificators", new ComputeIDsForAnalysisF(propertiesSourceProps), new UpdateNotificationStatusesF(propertiesSourceProps));
	}
	
	public static Cache<Map<String, Monitor>> getCache() {
		return cachedMonitors;
	}

	public static void initCache(Properties propertiesSourceProps) throws ConfigurationException {
		Properties.initCache(propertiesSourceProps);
		
		getCache().setExpiration(Properties.getCache().getExpirationPeriod());
	}

}
