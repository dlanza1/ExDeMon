package ch.cern.exdemon.monitor;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.log4j.Logger;
import org.apache.spark.streaming.State;

import ch.cern.exdemon.components.Component;
import ch.cern.exdemon.components.ComponentType;
import ch.cern.exdemon.components.ComponentTypes;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentBuildResult;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.filter.MetricsFilter;
import ch.cern.exdemon.monitor.analysis.Analysis;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;
import ch.cern.exdemon.monitor.analysis.types.NoneAnalysis;
import ch.cern.exdemon.monitor.trigger.Trigger;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.status.HasStatus;
import ch.cern.spark.status.StatusValue;
import ch.cern.utils.Pair;
import lombok.Getter;
import lombok.ToString;

@ToString
@ComponentType(Type.MONITOR)
public class Monitor extends Component{
    
    private static final long serialVersionUID = 508898995309159431L;

    private final static Logger LOG = Logger.getLogger(Monitor.class.getName());
    
    @Getter
    private MetricsFilter filter;

    private Analysis analysis;
    
    @Getter
    protected Map<String, Trigger> triggers;

	private Map<String, String> tags;

    private Map<String, String> fixedValueAttributes;
    
    public Monitor(){
    }
    
    public Monitor(String id){
        setId(id);
    }
    
    @Override
    public void config(Properties properties) throws ConfigurationException {
        filter = MetricsFilter.build(properties.getSubset("filter"));
        
        fixedValueAttributes = properties.getSubset("attribute").entrySet().stream()
                .map(entry -> new Pair<String, String>(entry.getKey().toString(), entry.getValue().toString()))
                .collect(Collectors.toMap(Pair::first, Pair::second));
        
        Properties analysis_props = properties.getSubset("analysis");
        if(!analysis_props.isTypeDefined())
            analysis_props.setProperty("type", NoneAnalysis.class.getAnnotation(RegisterComponentType.class).value());
    	
        ComponentBuildResult<Analysis> analysisBuildResult = ComponentTypes.build(Type.ANAYLSIS, analysis_props);
        analysisBuildResult.throwExceptionIfPresent();
        analysis = analysisBuildResult.getComponent().get();
        
    	Properties triggersProps = properties.getSubset("triggers");
        
        Set<String> triggerIds = triggersProps.getIDs();
        triggers = new HashMap<>();
        for (String triggerId : triggerIds) {
            Properties props = triggersProps.getSubset(triggerId);
        		
    		if(!props.isTypeDefined())
    		    props.setProperty("type", "statuses");
    		
    		ComponentBuildResult<Trigger> triggerBuildResult = ComponentTypes.build(Type.TRIGGER, triggerId, props);
    		triggerBuildResult.throwExceptionIfPresent();
    		triggers.put(triggerId, triggerBuildResult.getComponent().get());
		}
        
        tags = properties.getSubset("tags").toStringMap();
        
        properties.confirmAllPropertiesUsed();
    }

    public Optional<AnalysisResult> process(State<StatusValue> status, Metric metric) {
    	AnalysisResult result = null;

        try{
            if(analysis.hasStatus() && status.exists())
                ((HasStatus) analysis).load(status.get());
        		
            result = analysis.apply(metric);
            
            result.addAnalysisParam("type", analysis.getClass().getAnnotation(RegisterComponentType.class).value());
            
            if(analysis.hasStatus())
            		analysis.getStatus().ifPresent(s -> status.update(s));
        }catch(Throwable e){
            result = AnalysisResult.buildWithStatus(Status.EXCEPTION, e.getClass().getSimpleName() + ": " + e.getMessage());
            LOG.error(e.getMessage(), e);
        }
        
        metric.getAttributes().putAll(fixedValueAttributes);
        result.addAnalysisParam("monitor.name", getId());
        result.setAnalyzedMetric(metric);
        result.setTags(tags);

        return Optional.of(result);
    }
    
    public Map<String, String> getMetricIDs(Metric metric) {
		return metric.getAttributes();
	}

}
