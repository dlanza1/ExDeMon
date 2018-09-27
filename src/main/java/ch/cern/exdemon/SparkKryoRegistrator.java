package ch.cern.exdemon;

import org.apache.spark.serializer.KryoRegistrator;

import com.esotericsoftware.kryo.Kryo;

import ch.cern.exdemon.metrics.ValueHistory;
import ch.cern.exdemon.metrics.defined.DefinedMetricStatuskey;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.exdemon.metrics.defined.equation.var.agg.AggregationValues;
import ch.cern.exdemon.monitor.MonitorStatusKey;
import ch.cern.exdemon.monitor.trigger.TriggerStatus;
import ch.cern.exdemon.monitor.trigger.TriggerStatusKey;

public class SparkKryoRegistrator implements KryoRegistrator {

    @Override
    public void registerClasses(Kryo kryo) {
        kryo.register(DefinedMetricStatuskey.class);
        kryo.register(DefinedMetricStatuskey.class); 
        kryo.register(MonitorStatusKey.class);
        kryo.register(TriggerStatusKey.class);
        kryo.register(TriggerStatus.class); 
        kryo.register(ValueHistory.class);
        kryo.register(VariableStatuses.class);
        kryo.register(AggregationValues.class);
        
        kryo.register(ValueHistory.class, new ValueHistory.KryoSerializer());
    }
    
}
