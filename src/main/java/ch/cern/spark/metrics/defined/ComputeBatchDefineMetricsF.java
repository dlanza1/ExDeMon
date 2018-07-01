package ch.cern.spark.metrics.defined;

import java.time.Instant;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.streaming.Time;

import ch.cern.components.ComponentsCatalog;
import ch.cern.components.Component.Type;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.defined.equation.var.VariableStatuses;
import scala.Tuple2;

public class ComputeBatchDefineMetricsF implements FlatMapFunction<Tuple2<DefinedMetricStatuskey, VariableStatuses>, Metric> {

	private static final long serialVersionUID = 3779814069810467993L;

	private Instant time;

	private Properties propertiesSourceProps;
	
	public ComputeBatchDefineMetricsF(Time time, Properties propertiesSourceProps) {
        this.time = Instant.ofEpochMilli(time.milliseconds());
        this.propertiesSourceProps = propertiesSourceProps;
    }

	@Override
	public Iterator<Metric> call(Tuple2<DefinedMetricStatuskey, VariableStatuses> pair) throws Exception {
		ComponentsCatalog.init(propertiesSourceProps);
		
        List<Metric> result = new LinkedList<>();

        DefinedMetricStatuskey ids = pair._1;
        VariableStatuses store = pair._2;
        
        Optional<DefinedMetric> definedMetricOpt = ComponentsCatalog.get(Type.METRIC, ids.getID());
        if(!definedMetricOpt.isPresent())
            return result.iterator();
        DefinedMetric definedMetric = definedMetricOpt.get();
        
        Optional<Metric> metricOpt = definedMetric.generateByBatch(store, time, ids.getMetric_attributes());
        
        metricOpt.ifPresent(result::add);
        
        return result.iterator();
	}

}
