package ch.cern.spark.metrics;

import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.spark.metrics.monitors.Monitor;
import ch.cern.spark.metrics.monitors.Monitors;
import scala.Tuple2;

public class ComputeIDsForMetricsF implements PairFlatMapFunction<Metric, MonitorIDMetricIDs, Metric>{
    
    private static final long serialVersionUID = 2181051149359938177L;
    
    private Monitors monitorsCache;

    public ComputeIDsForMetricsF(Monitors monitorsCache) {
        this.monitorsCache = monitorsCache;
    }

    @Override
    public Iterator<Tuple2<MonitorIDMetricIDs, Metric>> call(Metric metric) throws Exception {
        return monitorsCache.values().stream()
	        		.filter(monitor -> monitor.getFilter().test(metric))
	        		.map(Monitor::getId)
	        		.map(monitorID -> new MonitorIDMetricIDs(monitorID, metric.getIDs()))
	        		.map(ids -> new Tuple2<MonitorIDMetricIDs, Metric>(ids, metric))
	        		.iterator();
    }

    public static JavaPairDStream<MonitorIDMetricIDs, Metric> apply(Monitors monitorsCache, JavaDStream<Metric> metricsS) {
        JavaPairDStream<MonitorIDMetricIDs, Metric> metricsWithIDs = 
                metricsS.flatMapToPair(new ComputeIDsForMetricsF(monitorsCache));
        
        return metricsWithIDs;
    }

}
