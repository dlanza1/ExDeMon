package ch.cern.spark.metrics.monitor;

import java.io.IOException;
import java.io.Serializable;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;

import ch.cern.spark.Component;
import ch.cern.spark.Component.Type;
import ch.cern.spark.ComponentManager;
import ch.cern.spark.Pair;
import ch.cern.spark.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.analysis.Analysis;
import ch.cern.spark.metrics.filter.Filter;
import ch.cern.spark.metrics.notificator.Notificator;
import ch.cern.spark.metrics.preanalysis.PreAnalysis;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.metrics.store.MetricStore;
import ch.cern.spark.metrics.store.Store;

public class Monitor implements Serializable{

    private static final long serialVersionUID = -3695000414623885877L;
    
    private final static Logger LOG = Logger.getLogger(Monitor.class.getName());
    
    protected String id;
    
    private Filter filter;

    private Properties preAnalysisProps;

    private Properties analysisProps;
    
    private Properties notificatorsProps;

    public static String MAX_PERIOD_PARAM = "missing.max-period";
    private Optional<Duration> maximumMissingPeriod;
    
    public Monitor(String id){
        this.id = id;
    }
    
    public Monitor config(Properties properties) {
        filter = Filter.build(properties.getSubset("filter"));
        
        preAnalysisProps = properties.getSubset("pre-analysis");
        analysisProps = properties.getSubset("analysis");
        notificatorsProps = properties.getSubset("notificator");
        
        maximumMissingPeriod = properties.getPeriod(MAX_PERIOD_PARAM);
        
        return this;
    }

    public AnalysisResult process(MetricStore store, Metric metric) throws Exception {
    	
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
        
        preAnalysis.flatMap(Component::getStore).ifPresent(store::setPreAnalysisStore);
        analysis.getStore().ifPresent(store::setAnalysisStore);

        return result;
    }
    
    public Filter getFilter(){
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
    
    public static Map<String, Monitor> getAll(Properties.PropertiesCache propertiesExp) throws IOException {
        Properties properties = propertiesExp.get().getSubset("monitor");
        
        Set<String> monitorNames = properties.getUniqueKeyFields();
        
        Map<String, Monitor> monitors = monitorNames.stream()
        		.map(id -> new Pair<String, Properties>(id, properties.getSubset(id)))
        		.map(info -> new Monitor(info.first).config(info.second))
        		.collect(Collectors.toMap(Monitor::getId, m -> m));
        
        LOG.info("Monitors: " + monitors);
        
        return monitors;
    }

    public Optional<Duration> getMaximumMissingPeriod() {
        return maximumMissingPeriod;
    }
    
    public Set<String> getNotificatorIDs(){
        return notificatorsProps.getUniqueKeyFields();
    }

    public Notificator getNotificator(String id, Optional<Store> store) throws Exception {
        Properties props = notificatorsProps.getSubset(id);
        
        return ComponentManager.build(Type.NOTIFICATOR, store, props);
    }

}
