package ch.cern.spark.metrics.defined;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.log4j.Logger;

import ch.cern.Cache;
import ch.cern.Properties;
import ch.cern.Properties.PropertiesCache;
import ch.cern.spark.Pair;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.Metric;

public class DefinedMetrics extends Cache<Map<String, DefinedMetric>> implements Serializable {

	private static final long serialVersionUID = -6131690270497529514L;
	
	private transient final static Logger LOG = Logger.getLogger(DefinedMetrics.class.getName());
	
	private PropertiesCache propertiesCache;

	public DefinedMetrics(PropertiesCache propertiesCache) {
		this.propertiesCache = propertiesCache;
	}

	@Override
	protected Map<String, DefinedMetric> load() throws Exception {
        Properties properties = propertiesCache.get().getSubset("metrics.define");
        
        Set<String> metricsDefinedNames = properties.getUniqueKeyFields();
        
        Map<String, DefinedMetric> definedMetrics = metricsDefinedNames.stream()
        		.map(id -> new Pair<String, Properties>(id, properties.getSubset(id)))
        		.map(info -> {
					try {
						return new DefinedMetric(info.first).config(info.second);
					} catch (ConfigurationException e) {
						LOG.error("ID " + info.first + ":" + e.getMessage(), e);
						return null;
					}
				})
        		.filter(out -> out != null)
        		.collect(Collectors.toMap(DefinedMetric::getName, m -> m));
        
        LOG.info("Metrics defined: " + definedMetrics);
        
        return definedMetrics;
	}
	
	public Stream<Metric> generate(Stream<Metric> metrics) throws ClassNotFoundException, IOException{
        return metrics.mapWithState("definedMetrics", new ComputeIDsForDefinedMetricsF(this), new UpdateDefinedMetricStatusesF(this));
	}

}
