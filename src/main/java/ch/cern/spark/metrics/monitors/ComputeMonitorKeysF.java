package ch.cern.spark.metrics.monitors;

import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import ch.cern.components.Component.Type;
import ch.cern.components.ComponentsCatalog;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import scala.Tuple2;

public class ComputeMonitorKeysF implements PairFlatMapFunction<Metric, MonitorStatusKey, Metric>{
    
    private static final long serialVersionUID = 2181051149359938177L;
    
    private Properties componentsSourceProperties;
    
    public ComputeMonitorKeysF(Properties componentsSourceProperties) {
        this.componentsSourceProperties = componentsSourceProperties;
    }

    @Override
    public Iterator<Tuple2<MonitorStatusKey, Metric>> call(Metric metric) throws Exception {
        ComponentsCatalog.init(componentsSourceProperties);
    	
        Map<String, Monitor> monitors = ComponentsCatalog.get(Type.MONITOR);
        
        return monitors.values().stream()
	        		.filter(monitor -> monitor.getFilter().test(metric))
	        		.map(monitor -> new MonitorStatusKey(monitor.getId(), monitor.getMetricIDs(metric)))
	        		.map(ids -> new Tuple2<MonitorStatusKey, Metric>(ids, metric))
	        		.iterator();
    }
    
}
