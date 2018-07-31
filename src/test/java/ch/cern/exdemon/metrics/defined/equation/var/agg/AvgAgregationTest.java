package ch.cern.exdemon.metrics.defined.equation.var.agg;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.LinkedList;

import org.junit.Test;

import ch.cern.exdemon.metrics.DatedValue;
import ch.cern.exdemon.metrics.value.AggregatedValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.Value;

public class AvgAgregationTest {

    private AvgAggregation agg = new AvgAggregation();
    
    @Test
    public void count() {
        LinkedList<DatedValue> values = new LinkedList<DatedValue>();
        values.add(new DatedValue(Instant.EPOCH, new FloatValue(10)));
        values.add(new DatedValue(Instant.EPOCH, new FloatValue(20)));
        values.add(new DatedValue(Instant.EPOCH, new FloatValue(30)));
        
        Value result = agg.aggregateValues(values, Instant.EPOCH);
        
        assertEquals(20, result.getAsAggregated().get().getAsFloat().get(), 0f);
    }
    
    @Test
    public void countWithAggValues() {
        LinkedList<DatedValue> values = new LinkedList<DatedValue>();
        values.add(new DatedValue(Instant.EPOCH, new AggregatedValue(new FloatValue(10))));
        values.add(new DatedValue(Instant.EPOCH, new AggregatedValue(new FloatValue(20))));
        values.add(new DatedValue(Instant.EPOCH, new FloatValue(10)));
        values.add(new DatedValue(Instant.EPOCH, new FloatValue(20)));
        values.add(new DatedValue(Instant.EPOCH, new FloatValue(30)));
        
        Value result = agg.aggregateValues(values, Instant.EPOCH);
        
        assertEquals(18, result.getAsAggregated().get().getAsFloat().get(), 0f);
    }
    
}
