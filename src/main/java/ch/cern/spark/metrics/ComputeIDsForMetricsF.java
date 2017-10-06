package ch.cern.spark.metrics;

import java.util.Iterator;
import java.util.LinkedList;

import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.streaming.api.java.JavaPairDStream;

import ch.cern.spark.Properties.Expirable;
import ch.cern.spark.metrics.filter.Filter;
import ch.cern.spark.metrics.monitor.Monitor;
import scala.Tuple2;

public class ComputeIDsForMetricsF implements PairFlatMapFunction<Metric, MonitorIDMetricIDs, Metric>{
    
    private static final long serialVersionUID = 2181051149359938177L;

    private LinkedList<Monitor> monitors = null;

    private Expirable propertiesExp;

    public ComputeIDsForMetricsF(Expirable propertiesExp) {
        this.propertiesExp = propertiesExp;
    }

    @Override
    public Iterator<Tuple2<MonitorIDMetricIDs, Metric>> call(Metric metric) throws Exception {
        setMonitors();
        
        LinkedList<Tuple2<MonitorIDMetricIDs, Metric>> ids = new LinkedList<>();
        
        for (Monitor monitor : monitors) {
            Filter filter = monitor.getFilter();
            
            if(filter.apply(metric)){
                MonitorIDMetricIDs id = new MonitorIDMetricIDs(monitor.getId(), metric.getIDs());
                
                ids.add(new Tuple2<MonitorIDMetricIDs, Metric>(id, metric));
            }
        }
        
        return ids.iterator();
    }
    
    private void setMonitors() {
        if(monitors == null)
            monitors = new LinkedList<Monitor>(Monitor.getAll(propertiesExp).values());
    }

    public static JavaPairDStream<MonitorIDMetricIDs, Metric> apply(Expirable propertiesExp, MetricsS metricsS) {
        JavaPairDStream<MonitorIDMetricIDs, Metric> metricsWithIDs = 
                metricsS.stream().flatMapToPair(new ComputeIDsForMetricsF(propertiesExp));
        
        return metricsWithIDs;
    }

}
