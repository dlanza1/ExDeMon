package ch.cern.spark.metrics;

import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.monitors.Monitors;
import scala.Tuple2;

public class ComputeIDsForMetricsF implements PairFlatMapFunction<Metric, MonitorIDMetricIDs, Metric>{
    
    private static final long serialVersionUID = 2181051149359938177L;
    
    private Properties propertiesSourceProperties;
    
    public ComputeIDsForMetricsF(Properties propertiesSourceProperties) {
    		this.propertiesSourceProperties = propertiesSourceProperties;
    }

    @Override
    public Iterator<Tuple2<MonitorIDMetricIDs, Metric>> call(Metric metric) throws Exception {
    		Monitors.initCache(propertiesSourceProperties);
    	
        return Monitors.getCache().get().values().stream()
	        		.filter(monitor -> monitor.getFilter().test(metric))
	        		.map(monitor -> new MonitorIDMetricIDs(monitor.getId(), monitor.getMetricIDs(metric)))
	        		.map(ids -> new Tuple2<MonitorIDMetricIDs, Metric>(ids, metric))
	        		.iterator();
    }
    
}
