package ch.cern.exdemon.metrics.defined.equation.var.agg;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.LinkedList;

import org.junit.Test;

import ch.cern.exdemon.metrics.DatedValue;
import ch.cern.exdemon.metrics.value.AggregatedValue;
import ch.cern.exdemon.metrics.value.BooleanValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.metrics.value.Value;

public class CountAgregationTest {

    private CountAgregation agg = new CountAgregation();
    
    @Test
    public void count() {
        LinkedList<DatedValue> values = new LinkedList<DatedValue>();
        values.add(new DatedValue(Instant.EPOCH, new FloatValue(0.2314)));
        values.add(new DatedValue(Instant.EPOCH, new StringValue("egerg")));
        values.add(new DatedValue(Instant.EPOCH, new BooleanValue(true)));
        
        Value result = agg.aggregateValues(values, Instant.EPOCH);
        
        assertEquals(values.size(), result.getAsAggregated().get().getAsFloat().get(), 0f);
    }
    
    @Test
    public void countWithAggValues() {
        LinkedList<DatedValue> values = new LinkedList<DatedValue>();
        values.add(new DatedValue(Instant.EPOCH, new AggregatedValue(new FloatValue(5))));
        values.add(new DatedValue(Instant.EPOCH, new AggregatedValue(new FloatValue(2))));
        values.add(new DatedValue(Instant.EPOCH, new FloatValue(932845)));
        values.add(new DatedValue(Instant.EPOCH, new StringValue("23fdw")));
        values.add(new DatedValue(Instant.EPOCH, new BooleanValue(false)));
        
        Value result = agg.aggregateValues(values, Instant.EPOCH);
        
        assertEquals(10, result.getAsAggregated().get().getAsFloat().get(), 0f);
    }
    
}
