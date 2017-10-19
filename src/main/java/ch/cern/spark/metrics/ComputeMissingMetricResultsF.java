package ch.cern.spark.metrics;

import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Time;

import ch.cern.spark.metrics.monitors.Monitor;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.store.MetricStore;
import scala.Tuple2;

public class ComputeMissingMetricResultsF implements FlatMapFunction<Tuple2<MonitorIDMetricIDs, MetricStore>, AnalysisResult> {

    private static final long serialVersionUID = 806231785227390268L;
    
    private Monitors monitorsCache;

    private Instant time;
    
    public ComputeMissingMetricResultsF(Monitors monitorsCache, Time time) {
        this.monitorsCache = monitorsCache;
        
        this.time = Instant.ofEpochMilli(time.milliseconds());
    }
    
    @Override
    public Iterator<AnalysisResult> call(Tuple2<MonitorIDMetricIDs, MetricStore> pair)
            throws Exception {
        
        List<AnalysisResult> result = new LinkedList<>();

        MonitorIDMetricIDs ids = pair._1;
        MetricStore store = pair._2;
        
        Optional<Monitor> monitorOpt = monitorsCache.get(ids.getMonitorID());
        if(!monitorOpt.isPresent())
            return result.iterator();
        Monitor monitor = monitorOpt.get();
        
        Optional<Duration> maximumMissingPeriod = monitor.getMaximumMissingPeriod();
        
        Duration elapsedTime = store.elapsedTimeFromLastMetric(time);
        
        if(maximumMissingPeriod.isPresent() && elapsedTime.compareTo(maximumMissingPeriod.get()) > 0)
            result.add(AnalysisResult.buildMissingMetric(ids, monitor, time, elapsedTime));
        
        return result.iterator();
    }

}
