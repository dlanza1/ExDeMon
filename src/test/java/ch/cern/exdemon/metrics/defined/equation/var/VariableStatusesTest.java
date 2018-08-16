package ch.cern.exdemon.metrics.defined.equation.var;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;

import org.junit.Test;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.ValueHistory;
import ch.cern.exdemon.metrics.defined.equation.ComputationException;
import ch.cern.exdemon.metrics.defined.equation.var.agg.AggregationValues;
import ch.cern.exdemon.metrics.defined.equation.var.agg.AvgAggregation;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.spark.status.storage.JSONStatusSerializer;

public class VariableStatusesTest {

    @Test
    public void jsonSerialization() throws IOException, ComputationException {
        JSONStatusSerializer serializer = new JSONStatusSerializer();
        
        VariableStatuses value = new VariableStatuses();
        
        AggregationValues aggregationValues = new AggregationValues(100, 10);
        aggregationValues.add(10, 7, Instant.now());
        aggregationValues.add(23, new StringValue("string-value"), Instant.now(), new Metric(Instant.now(), 0, new HashMap<>()), new Metric(Instant.now(), 0, new HashMap<>()));
        VariableStatus var1status = new ValueVariable.Status_(aggregationValues);
        value.put("var1", var1status);
        
        ValueHistory valueHistory = new ValueHistory(10, 10, ChronoUnit.NANOS, new AvgAggregation());
        valueHistory.add(Instant.now(), 115);
        valueHistory.add(Instant.now(), 5324);
        VariableStatus var2status = new ValueVariable.Status_(valueHistory);
        value.put("var2", var2status);
        
        AttributeVariable.Status_ var3status = new AttributeVariable.Status_();
        var3status.value = "att-var-value";
        value.put("var3", var3status);
        
        FixedValueVariable.Status_ var4status = new FixedValueVariable.Status_();
        value.put("var4", var4status);
        
        byte[] valueSerialized = serializer.fromValue(value);
        VariableStatuses valueResult = (VariableStatuses) serializer.toValue(valueSerialized);
        
//        assertEquals(value.get("var1"), valueResult.get("var1"));
//        assertEquals(value.get("var2"), valueResult.get("var2"));
//        assertEquals(value.get("var3"), valueResult.get("var3"));
//        assertEquals(value.get("var4"), valueResult.get("var4"));
        assertEquals(value, valueResult);
    }
    
}
