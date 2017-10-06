package ch.cern.spark.metrics;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Time;

import ch.cern.spark.Properties.Expirable;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.monitor.Monitor;
import ch.cern.spark.metrics.store.MetricStore;
import scala.Tuple2;

public class ComputeMissingMetricResultsF implements FlatMapFunction<Tuple2<MonitorIDMetricIDs,MetricStore>, AnalysisResult> {

    private static final long serialVersionUID = 806231785227390268L;
    
    private Expirable propertiesExp;

    private Time time;

    private Map<String, Monitor> monitors = null;
    
    public ComputeMissingMetricResultsF(Expirable propertiesExp, Time time) {
        this.propertiesExp = propertiesExp;
        this.time = time;
    }
    
    @Override
    public Iterator<AnalysisResult> call(Tuple2<MonitorIDMetricIDs, MetricStore> pair)
            throws Exception {
        
        List<AnalysisResult> result = new LinkedList<>();

        MonitorIDMetricIDs ids = pair._1;
        MetricStore store = pair._2;
        
        Monitor monitor = getMonitor(ids.getMonitorID());
        if(monitor == null)
            return result.iterator();
        
        Long maximumMissingPeriod = monitor.getMaximumMissingPeriod();
        
        long elapsedTime = store.elapsedTimeFromLastMetric(time);
        
        if(maximumMissingPeriod != null && elapsedTime > maximumMissingPeriod)
            result.add(AnalysisResult.buildMissingMetric(ids, monitor, time, elapsedTime));
        
        return result.iterator();
    }
    
    private Monitor getMonitor(String monitorID) {
        if(monitors == null)
            monitors = Monitor.getAll(propertiesExp);
        
        return monitors.get(monitorID);
    }

}
