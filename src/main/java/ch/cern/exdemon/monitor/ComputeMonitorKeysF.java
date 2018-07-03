package ch.cern.exdemon.monitor;

import java.util.Iterator;
import java.util.Map;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import ch.cern.exdemon.components.ComponentsCatalog;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.properties.Properties;
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
