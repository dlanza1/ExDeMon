package ch.cern.spark.metrics.monitors;

import java.time.Duration;
import java.util.HashMap;
import java.util.Optional;
import java.util.Set;

import org.apache.log4j.Logger;

import ch.cern.Component;
import ch.cern.Component.Type;
import ch.cern.ComponentManager;
import ch.cern.ConfigurationException;
import ch.cern.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.analysis.Analysis;
import ch.cern.spark.metrics.filter.MetricsFilter;
import ch.cern.spark.metrics.notificator.Notificator;
import ch.cern.spark.metrics.preanalysis.PreAnalysis;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.metrics.store.MetricStore;
import ch.cern.spark.metrics.store.Store;
import ch.cern.utils.TimeUtils;

public class Monitor {
    
    private final static Logger LOG = Logger.getLogger(Monitor.class.getName());
    
    private String id;
    
    private MetricsFilter filter;

    private Properties preAnalysisProps;

    private Properties analysisProps;
    
    private Properties notificatorsProps;

    public static String MAX_PERIOD_PARAM = "missing.max-period";
    private String maximumMissingPeriodPropertyValue;
    private Optional<Duration> maximumMissingPeriod;

	private HashMap<String, String> tags;
    
    public Monitor(String id){
        this.id = id;
    }
    
    public Monitor config(Properties properties) throws ConfigurationException {
        filter = MetricsFilter.build(properties.getSubset("filter"));
        
        preAnalysisProps = properties.getSubset("pre-analysis");
        analysisProps = properties.getSubset("analysis");
        notificatorsProps = properties.getSubset("notificator");
        
        maximumMissingPeriodPropertyValue = properties.getProperty(MAX_PERIOD_PARAM);
        
        tags = new HashMap<>();
        Properties tagsProps = properties.getSubset("tags");
        Set<String> tagKeys = tagsProps.getUniqueKeyFields();
        tagKeys.forEach(key -> tags.put(key, tagsProps.getProperty(key)));
        
        return this;
    }

    public AnalysisResult process(MetricStore store, Metric metric) throws Exception {
    		setMaximumMissingPeriod();
    	
    		Optional<PreAnalysis> preAnalysis = ComponentManager.buildOptional(Type.PRE_ANALYSIS, store.getPreAnalysisStore(), preAnalysisProps);
    		Analysis analysis = ComponentManager.build(Type.ANAYLSIS, store.getAnalysisStore(), analysisProps);
    		
        AnalysisResult result = null;
        
        try{
        		Optional<Metric> preAnalyzedValue = preAnalysis.flatMap(metric::map);

            result = preAnalyzedValue.orElse(metric).map(analysis).get();
            
            if(preAnalyzedValue.isPresent())
            		result.addMonitorParam("preAnalyzedValue", preAnalyzedValue.get().getValue());
        }catch(Throwable e){
            result = AnalysisResult.buildWithStatus(Status.EXCEPTION, e.getClass().getSimpleName() + ": " + e.getMessage());
            LOG.error(e.getMessage(), e);
        }
        
        result.addMonitorParam("name", id);
        result.addMonitorParam("type", analysisProps.getProperty("type"));
        result.setTags(tags);
        
        preAnalysis.flatMap(Component::getStore).ifPresent(store::setPreAnalysisStore);
        analysis.getStore().ifPresent(store::setAnalysisStore);

        return result;
    }
    
    private void setMaximumMissingPeriod() throws ConfigurationException {
    		if(maximumMissingPeriodPropertyValue == null)
    			return;
    		
    		try {
    			maximumMissingPeriod = Optional.of(TimeUtils.parsePeriod(maximumMissingPeriodPropertyValue));
    		}catch(NumberFormatException e){
    			throw new ConfigurationException("For key=" + MAX_PERIOD_PARAM + ": " + e.getMessage());
    		}
	}

	public MetricsFilter getFilter(){
        return filter;
    }

    @Override
    public String toString() {
        return "MetricMonitor [id=" + id + ", filter=" + filter + "]";
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Optional<Duration> getMaximumMissingPeriod() throws ConfigurationException {
    		setMaximumMissingPeriod();
    	
        return maximumMissingPeriod == null ? Optional.empty() : maximumMissingPeriod;
    }
    
    public Set<String> getNotificatorIDs(){
        return notificatorsProps.getUniqueKeyFields();
    }

    public Notificator getNotificator(String id, Optional<Store> store) throws Exception {
        Properties props = notificatorsProps.getSubset(id);
        
        return ComponentManager.build(Type.NOTIFICATOR, store, props);
    }

}
