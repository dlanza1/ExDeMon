package ch.cern.spark.metrics.monitor;

import java.io.IOException;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;

import ch.cern.spark.Component.Type;
import ch.cern.spark.ComponentManager;
import ch.cern.spark.Properties;
import ch.cern.spark.StringUtils;
import ch.cern.spark.metrics.analysis.Analysis;
import ch.cern.spark.metrics.filter.Filter;
import ch.cern.spark.metrics.notificator.Notificator;
import ch.cern.spark.metrics.preanalysis.PreAnalysis;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.spark.metrics.store.HasStore;
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
    private Long maximumMissingPeriod;
    
    public Monitor(String id){
        this.id = id;
    }
    
    public Monitor config(Properties properties) {
        filter = Filter.build(properties.getSubset("filter"));
        
        preAnalysisProps = properties.getSubset("pre-analysis");
        analysisProps = properties.getSubset("analysis");
        notificatorsProps = properties.getSubset("notificator");
        
        String missingMetricMaxPeriodInSeconds_config = properties.getProperty(MAX_PERIOD_PARAM);
        if(missingMetricMaxPeriodInSeconds_config != null)
            maximumMissingPeriod = StringUtils.parseStringWithTimeUnitToSeconds(missingMetricMaxPeriodInSeconds_config);
        
        return this;
    }

    public AnalysisResult process(MetricStore store, Date timestamp, Float value) throws Exception {
        AnalysisResult result = null;
        
        try{
            Float preAnalyzedValue = preAnalysis(store, timestamp, value);
            
            result = analysis(store, timestamp, preAnalyzedValue != null ? preAnalyzedValue : value);
            
            result.addMonitorParam("preAnalyzedValue", preAnalyzedValue);
        }catch(Throwable e){
            result = AnalysisResult.buildWithStatus(Status.EXCEPTION, e.getClass().getSimpleName() + ": " + e.getMessage());
            LOG.error(e.getMessage(), e);
        }
        
        result.addMonitorParam("name", id);
        result.addMonitorParam("type", analysisProps.getProperty("type"));

        return result;
    }

    private AnalysisResult analysis(MetricStore store, Date timestamp, Float value) throws Exception {
        Analysis analysis = (Analysis) ComponentManager.build(Type.ANAYLSIS, store.getAnalysisStore(), analysisProps);
        
        AnalysisResult result = analysis.process(timestamp, value);
        
        if(analysis instanceof HasStore)
            store.setAnalysisStore(((HasStore) analysis).save());
        
        return result;
    }

    private Float preAnalysis(MetricStore store, Date timestamp, Float value) throws Exception {
        if(preAnalysisProps.getProperty("type") != null){
            PreAnalysis preAnalysis = (PreAnalysis) ComponentManager.build(Type.PRE_ANALYSIS, store.getPreAnalysisStore(), preAnalysisProps);
         
            value = preAnalysis.process(timestamp, value);
            
            if(preAnalysis instanceof HasStore)
                store.setPreAnalysisStore(((HasStore) preAnalysis).save());
            
            return value;
        }else{
            return null;
        }
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
    
    public static Map<String, Monitor> getAll(Properties.Expirable propertiesExp) {
        Map<String, Monitor> monitors = new HashMap<>();
        
        Properties properties;
        try {
            properties = propertiesExp.get();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        
        Properties monitorsProperties = properties.getSubset("monitor");
        
        Set<String> monitorNames = monitorsProperties.getUniqueKeyFields(0);
        
        for (String monitorName : monitorNames) {
            Properties monitorProps = monitorsProperties.getSubset(monitorName);
            
            Monitor metricMonitor = new Monitor(monitorName);
            metricMonitor.config(monitorProps);
            
            monitors.put(monitorName, metricMonitor);
        }
        
        LOG.info("Monitors: " + monitors);
        
        return monitors;
    }

    public Long getMaximumMissingPeriod() {
        return maximumMissingPeriod;
    }
    
    public Set<String> getNotificatorIDs(){
        return notificatorsProps.getUniqueKeyFields(0);
    }

    public Notificator getNotificator(String id, Store store) throws Exception {
        Properties props = notificatorsProps.getSubset(id);
        
        return (Notificator) ComponentManager.build(Type.NOTIFICATOR, store, props);
    }

}
