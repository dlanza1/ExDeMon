package ch.cern.spark.metrics.monitors;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.State;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentManager;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.filter.MetricsFilter;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;
import lombok.ToString;

@ToString
public class InErrorMonitor extends Monitor {
    
    private final static Logger LOG = Logger.getLogger(InErrorMonitor.class.getName());

	private Exception exception;
	
	private MetricsFilter filter;

	private Map<String, String> tags;
	
	public InErrorMonitor(String id, ConfigurationException e) {
		super(id);
		
		exception = e;
	}

	@Override
	public Monitor config(Properties properties) {
		try {
			filter = MetricsFilter.build(properties.getSubset("filter"));
		} catch (ConfigurationException e) {
		    LOG.error(e);
		}
		
		tags = new HashMap<>();
        Properties tagsProps = properties.getSubset("tags");
        Set<String> tagKeys = tagsProps.getIDs();
        tagKeys.forEach(key -> tags.put(key, tagsProps.getProperty(key)));
        
        Properties errorProps = properties.getSubset("notificator.$error");
        if(errorProps.size() == 0) {
            errorProps.setProperty("type", "constant");
            errorProps.setProperty("period", "10m");
            errorProps.setProperty("sinks", "ALL");
        }
        errorProps.setProperty("statuses", "EXCEPTION");
        try {
            notificators = new HashMap<>();
            notificators.put("$error", ComponentManager.build(Type.NOTIFICATOR, "$error", errorProps));
        } catch (ConfigurationException e) {
            LOG.error(e);
        }
		
		return this;
	}
	
	@Override
	public Optional<AnalysisResult> process(State<StatusValue> storeState, Metric metric) {
		AnalysisResult result = null;
		
		if(filter != null) {
			result = AnalysisResult.buildWithStatus(Status.EXCEPTION, exception.getClass().getSimpleName() + ": " + exception.getMessage());
		}else{
			Status_ store = null;
			
			if(storeState.exists() && storeState.get() instanceof Status_) {
				store = (Status_) storeState.get();
				
				if(store.hasExpired(metric.getTimestamp()))
					result = AnalysisResult.buildWithStatus(Status.EXCEPTION, exception.getClass().getSimpleName() + ": " + exception.getMessage());
			}else{
				store = new Status_();
				store.lastResult = metric.getTimestamp();
				
				result = AnalysisResult.buildWithStatus(Status.EXCEPTION, exception.getClass().getSimpleName() + ": " + exception.getMessage());
			}
			
			storeState.update(store);
		}

		if(result != null) {
			result.addAnalysisParam("monitor.name", id);
			result.setAnalyzedMetric(metric);
			result.setTags(tags);
		}
		
		return Optional.ofNullable(result);
	}
	
	@Override
	public MetricsFilter getFilter() {
		if(filter != null)
			return filter;
		else
			return new MetricsFilter();
	}
	
	@Override
	public Map<String, String> getMetricIDs(Metric metric) {
		if(filter != null)
			return metric.getAttributes();
		else
			return new HashMap<>();
	}
	
	@ClassNameAlias("in-error-notificator")
	private static class Status_ extends StatusValue {

		private static final long serialVersionUID = -6991497562392525744L;

		private Instant lastResult;

		public boolean hasExpired(Instant instant) {
			if(lastResult == null) {
				lastResult = instant;
				
				return true;
			}
			
			if(lastResult.plus(Duration.ofMinutes(1)).compareTo(instant) <= 0) {
				lastResult = instant;
				
				return true;
			}else{
				return false;
			}
		}
		
	}
	
}
