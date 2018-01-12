package ch.cern.spark.metrics.defined.equation.var.agg;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import ch.cern.spark.metrics.DatedValue;
import ch.cern.spark.metrics.defined.equation.ComputationException;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.ClassNameAlias;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@ClassNameAlias("aggregated-values")
@ToString
@EqualsAndHashCode(callSuper=false)
public class AggregationValues extends StatusValue {

    private static final long serialVersionUID = -3277697169862561894L;
    
    private Map<Integer, DatedValue> values;

    @Getter @Setter
    private int max_aggregation_size;

    public AggregationValues(int max_aggregation_size) {
        this.max_aggregation_size = max_aggregation_size;
        
        values = new LinkedHashMap<>();
    }
    
    public void add(int hashCode, float f, Instant now) {
        add(hashCode, new FloatValue(f), now);   
    }
    
    public void add(int idHash, Value value, Instant instant) {
        value = Value.clone(value);

        // Removing the oldest entry if max size
        if (values.size() >= max_aggregation_size + 1)
            values.remove(values.keySet().iterator().next());

        values.put(idHash, new DatedValue(instant, value));
    }

    public Collection<DatedValue> getDatedValues() throws ComputationException {
        if(values.size() > max_aggregation_size)
            throw new ComputationException("Maximum aggregation size reached.");
        
        return values.values();        
    }

    public void purge(Instant oldestTime) {
        values.entrySet().removeIf(entry -> entry.getValue().getTime().isBefore(oldestTime));
    }

}
