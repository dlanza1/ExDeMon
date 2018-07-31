package ch.cern.exdemon.metrics.defined.equation.var.agg;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.LinkedList;

import org.junit.Test;

import ch.cern.exdemon.metrics.DatedValue;
import ch.cern.exdemon.metrics.value.AggregatedValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.Value;

public class DiffAgregationTest {

    private DiffAggregation agg = new DiffAggregation();
    
    @Test
    public void diff() {
        LinkedList<DatedValue> values = new LinkedList<DatedValue>();
        values.add(new DatedValue(Instant.ofEpochMilli(0), new FloatValue(9)));
        values.add(new DatedValue(Instant.ofEpochMilli(1), new FloatValue(20)));
        values.add(new DatedValue(Instant.ofEpochMilli(2), new FloatValue(30)));
        
        Value result = agg.aggregateValues(values, Instant.EPOCH);
        
        assertEquals(10, result.getAsAggregated().get().getAsFloat().get(), 0f);
    }
    
    @Test
    public void diffWithAggValuesShouldFail() {
        LinkedList<DatedValue> values = new LinkedList<DatedValue>();
        values.add(new DatedValue(Instant.ofEpochMilli(0), new AggregatedValue(new FloatValue(9))));
        values.add(new DatedValue(Instant.ofEpochMilli(1), new AggregatedValue(new FloatValue(20))));
        values.add(new DatedValue(Instant.ofEpochMilli(2), new FloatValue(34)));
        
        Value result = agg.aggregateValues(values, Instant.EPOCH);
        assertTrue(result.getAsException().isPresent());
        
        
        values = new LinkedList<DatedValue>();
        values.add(new DatedValue(Instant.ofEpochMilli(0), new AggregatedValue(new FloatValue(-1))));
        values.add(new DatedValue(Instant.ofEpochMilli(0), new AggregatedValue(new FloatValue(10))));
        values.add(new DatedValue(Instant.ofEpochMilli(1), new AggregatedValue(new FloatValue(-5))));
        
        result = agg.aggregateValues(values, Instant.EPOCH);
        assertTrue(result.getAsException().isPresent());
    }
    
}
