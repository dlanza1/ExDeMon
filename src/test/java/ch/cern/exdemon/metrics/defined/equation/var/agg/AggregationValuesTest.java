package ch.cern.exdemon.metrics.defined.equation.var.agg;

import static org.junit.Assert.fail;

import java.time.Duration;
import java.time.Instant;

import org.junit.Test;

import ch.cern.exdemon.metrics.defined.equation.ComputationException;

public class AggregationValuesTest {
    
    @Test
    public void shouldGenerateExceptionWhenMaxSizeIsReached() throws ComputationException {
        AggregationValues values = new AggregationValues(3, 0);
        
        Instant now = Instant.now();
        
        values.add(0, 0f, now);
        values.getDatedValues();
        
        values.add(1, 0f, now);
        values.getDatedValues();
        
        values.add(2, 0f, now);
        values.getDatedValues();
        
        values.add(3, 0f, now);
        try {
            values.getDatedValues();
            fail();
        }catch(Exception e) {}
    }
    
    @Test
    public void shouldRecoverAfterMaxSizeIsReached() throws ComputationException {
        AggregationValues values = new AggregationValues(3, 0);
        
        Instant now = Instant.now();
        
        values.add(0, 0f, now);
        values.getDatedValues();
        
        values.add(1, 0f, now.plus(Duration.ofSeconds(2)));
        values.getDatedValues();
        
        values.add(2, 0f, now.plus(Duration.ofSeconds(4)));
        values.getDatedValues();
        
        values.add(3, 0f, now.plus(Duration.ofSeconds(6)));
        try {
            values.getDatedValues();
            fail();
        }catch(Exception e) {}
        
        values.purge(now.plus(Duration.ofSeconds(1)));
        
        values.getDatedValues();
        
        values.add(4, 0f, now.plus(Duration.ofSeconds(6)));
        try {
            values.getDatedValues();
            fail();
        }catch(Exception e) {}
    }

}
