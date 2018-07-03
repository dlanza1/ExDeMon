package ch.cern.exdemon.metrics.defined.equation.var.agg;

import java.time.Instant;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

import ch.cern.exdemon.metrics.DatedValue;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.equation.ComputationException;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.Value;
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

    @Getter
    private Map<Integer, Metric> lastAggregatedMetrics;
    @Getter @Setter
    private int max_lastAggregatedMetrics_size = 0;
    
    public AggregationValues(int max_aggregation_size, int max_lastAggregatedMetrics_size) {
        this.max_aggregation_size = max_aggregation_size;
        this.max_lastAggregatedMetrics_size = max_lastAggregatedMetrics_size;
        
        values = new LinkedHashMap<>();
        
        if(max_lastAggregatedMetrics_size > 0)
            lastAggregatedMetrics = new LinkedHashMap<>();
    }
    
    public void add(int hashCode, float f, Instant now) {
        add(hashCode, new FloatValue(f), now);   
    }
    
	public void add(int hash, Value value, Instant timestamp, Metric metric, Metric originalMetric) {
		addLastAggMetric(hash, originalMetric);
		add(hash, value, timestamp);
	}
    
    private void addLastAggMetric(int hash, Metric originalMetric) {
        if(max_lastAggregatedMetrics_size <= 0) {
            lastAggregatedMetrics = null;
            return;
        }
        
        if(lastAggregatedMetrics == null)
            lastAggregatedMetrics = new LinkedHashMap<>();
        
        lastAggregatedMetrics.put(hash, originalMetric);
        
        while(lastAggregatedMetrics != null && lastAggregatedMetrics.size() >= max_lastAggregatedMetrics_size + 1)
            lastAggregatedMetrics.remove(lastAggregatedMetrics.keySet().iterator().next());
    }

    public void add(int idHash, Value value, Instant instant) {
        value = value.clone();

        // Removing the oldest entry if max size
        if (values.size() >= max_aggregation_size + 1) {
        	int key = values.keySet().iterator().next();
        	
            values.remove(key);
            if(lastAggregatedMetrics != null)
                lastAggregatedMetrics.remove(key);
        }

        values.put(idHash, new DatedValue(instant, value));
    }

    public Collection<DatedValue> getDatedValues() throws ComputationException {
        if(values.size() > max_aggregation_size)
            throw new ComputationException("Maximum aggregation size reached.");
        
        return values.values();        
    }

    public void purge(Instant oldestTime) {
        values.values().removeIf(dv -> dv.getTime().isBefore(oldestTime));
        
        if(lastAggregatedMetrics != null)
            lastAggregatedMetrics.values().removeIf(m -> m.getTimestamp().isBefore(oldestTime));
    }

    public void reset() {
        values = new LinkedHashMap<>();
        lastAggregatedMetrics = null;
    }

}
