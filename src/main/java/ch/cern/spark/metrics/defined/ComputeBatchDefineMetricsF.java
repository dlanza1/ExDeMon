package ch.cern.spark.metrics.defined;

import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Time;

import ch.cern.spark.metrics.Metric;
import scala.Tuple2;

public class ComputeBatchDefineMetricsF implements FlatMapFunction<Tuple2<DefinedMetricID, DefinedMetricStore>, Metric> {

	private static final long serialVersionUID = 3779814069810467993L;

	private Instant time;

	public ComputeBatchDefineMetricsF(Time time) {
        this.time = Instant.ofEpochMilli(time.milliseconds());
    }

	@Override
	public Iterator<Metric> call(Tuple2<DefinedMetricID, DefinedMetricStore> pair) throws Exception {
        List<Metric> result = new LinkedList<>();

        DefinedMetricID ids = pair._1;
        DefinedMetricStore store = pair._2;
        
        Optional<DefinedMetric> definedMetricOpt = Optional.ofNullable(DefinedMetrics.getCache().get().get(ids.getDefinedMetricName()));
        if(!definedMetricOpt.isPresent())
            return result.iterator();
        DefinedMetric definedMetric = definedMetricOpt.get();
        
        Optional<Metric> metricOpt = definedMetric.generateByBatch(store, time, ids.getGroupByMetricIDs());
        
        metricOpt.ifPresent(result::add);
        
        return result.iterator();
	}

}
