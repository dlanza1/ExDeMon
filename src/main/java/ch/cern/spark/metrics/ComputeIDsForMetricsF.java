package ch.cern.spark.metrics;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.spark.Properties.Expirable;
import ch.cern.spark.metrics.monitor.Monitor;
import scala.Tuple2;

public class ComputeIDsForMetricsF implements PairFlatMapFunction<Metric, MonitorIDMetricIDs, Metric>{
    
    private static final long serialVersionUID = 2181051149359938177L;

    private List<Monitor> monitors = null;

    private Expirable propertiesExp;

    public ComputeIDsForMetricsF(Expirable propertiesExp) {
        this.propertiesExp = propertiesExp;
    }

    @Override
    public Iterator<Tuple2<MonitorIDMetricIDs, Metric>> call(Metric metric) throws Exception {
        setMonitors();
        
        return monitors.stream()
	        		.filter(monitor -> monitor.getFilter().test(metric))
	        		.map(Monitor::getId)
	        		.map(monitorID -> new MonitorIDMetricIDs(monitorID, metric.getIDs()))
	        		.map(ids -> new Tuple2<MonitorIDMetricIDs, Metric>(ids, metric))
	        		.iterator();
    }
    
    private void setMonitors() throws IOException {
        if(monitors == null)
            monitors = new LinkedList<Monitor>(Monitor.getAll(propertiesExp).values());
    }

    public static JavaPairDStream<MonitorIDMetricIDs, Metric> apply(Expirable propertiesExp, MetricsS metricsS) {
        JavaPairDStream<MonitorIDMetricIDs, Metric> metricsWithIDs = 
                metricsS.flatMapToPair(new ComputeIDsForMetricsF(propertiesExp));
        
        return metricsWithIDs;
    }

}
