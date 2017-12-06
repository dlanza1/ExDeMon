package ch.cern.spark.metrics.monitors;

import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import scala.Tuple2;

public class ComputeMonitorKeysF implements PairFlatMapFunction<Metric, MonitorStatusKey, Metric>{
    
    private static final long serialVersionUID = 2181051149359938177L;
    
    private Properties propertiesSourceProperties;
    
    public ComputeMonitorKeysF(Properties propertiesSourceProperties) {
    		this.propertiesSourceProperties = propertiesSourceProperties;
    }

    @Override
    public Iterator<Tuple2<MonitorStatusKey, Metric>> call(Metric metric) throws Exception {
    		Monitors.initCache(propertiesSourceProperties);
    	
        return Monitors.getCache().get().values().stream()
	        		.filter(monitor -> monitor.getFilter().test(metric))
	        		.map(monitor -> new MonitorStatusKey(monitor.getId(), monitor.getMetricIDs(metric)))
	        		.map(ids -> new Tuple2<MonitorStatusKey, Metric>(ids, metric))
	        		.iterator();
    }
    
}
