package ch.cern.spark.metrics.defined.equation.var;

import static org.junit.Assert.assertEquals;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.Value;

public class MetricVariableTest  {
	
    @Test
    public void aggregationHitMaxSizeDuringPeriod() throws ConfigurationException {
        MetricVariable var = new MetricVariable("");
        Properties properties = new Properties();
        properties.setProperty("aggregate.type", "count");
        properties.setProperty("aggregate.max-size", "1000");
        properties.setProperty("aggregate.history.granularity", "m");
        properties.setProperty("ignore", "0h,h");
        properties.setProperty("expire", "1h,h");
        var.config(properties, Optional.empty());
        
        VariableStatuses variableStatuses = new VariableStatuses();
        
        Instant newest = Instant.parse("2007-12-03T10:00:00.00Z");
        Instant oldest = Instant.parse("2007-12-03T11:00:00.00Z").minus(Duration.ofMillis(1));
        int outOfPeriod = 0;
        
        int size = 20000;
        
        for (int i = 0; i < 20000; i++) {
            Instant time = Instant.parse("2007-12-03T09:40:00.00Z").plus(Duration.ofSeconds((long) (6000f * Math.random())));
            
            Metric metric = new Metric(time, new FloatValue(Math.random()), new HashMap<>());
            var.updateVariableStatuses(variableStatuses, metric);
            
            if(time.isBefore(newest) || time.isAfter(oldest))
                outOfPeriod++;
        }

        assertEquals(size - outOfPeriod, var.compute(variableStatuses, Instant.parse("2007-12-03T11:15:30.00Z")).getAsFloat().get(), 0f);
    }
    
    @Test
    public void aggregationSelectAttributes() throws ConfigurationException {
        MetricVariable var = new MetricVariable("");
        Properties properties = new Properties();
        properties.setProperty("aggregate.type", "count");
        properties.setProperty("aggregate.attributes", "seq");
        var.config(properties, Optional.empty());
        
        VariableStatuses variableStatuses = new VariableStatuses();

        Map<String, String> att = new HashMap<>();
        var.updateVariableStatuses(variableStatuses, new Metric(Instant.now(), 10f, att));
        assertEquals(0f, var.compute(variableStatuses, Instant.now()).getAsFloat().get(), 0f);
        
        att = new HashMap<>();
        att.put("noseq", "");
        var.updateVariableStatuses(variableStatuses, new Metric(Instant.now(), 10f, att));
        Value computed = var.compute(variableStatuses, Instant.now());
        assertEquals(0f, computed.getAsFloat().get(), 0f);
        assertEquals(0, computed.getLastSourceMetrics().size());
        
        att = new HashMap<>();
        att.put("seq", "1");
        var.updateVariableStatuses(variableStatuses, new Metric(Instant.now(), 10f, att));
        computed = var.compute(variableStatuses, Instant.now());
        assertEquals(1f, computed.getAsFloat().get(), 0f);
        assertEquals(1, computed.getLastSourceMetrics().size());
        
        att = new HashMap<>();
        att.put("seq", "2");
        var.updateVariableStatuses(variableStatuses, new Metric(Instant.now(), 10f, att));
        computed = var.compute(variableStatuses, Instant.now());
        assertEquals(2f, computed.getAsFloat().get(), 0f);
        assertEquals(2, computed.getLastSourceMetrics().size());
        
        att = new HashMap<>();
        att.put("seq", "1");
        var.updateVariableStatuses(variableStatuses, new Metric(Instant.now(), 10f, att));
        computed = var.compute(variableStatuses, Instant.now());
        assertEquals(2f, var.compute(variableStatuses, Instant.now()).getAsFloat().get(), 0f);
        assertEquals(2, computed.getLastSourceMetrics().size());
        
        att = new HashMap<>();
        att.put("seq", "1");
        att.put("noseq", "1");
        var.updateVariableStatuses(variableStatuses, new Metric(Instant.now(), 10f, att));
        computed = var.compute(variableStatuses, Instant.now());
        assertEquals(2f, computed.getAsFloat().get(), 0f);
        assertEquals(2, computed.getLastSourceMetrics().size());
        
        att = new HashMap<>();
        att.put("seq", "3");
        att.put("noseq", "1");
        var.updateVariableStatuses(variableStatuses, new Metric(Instant.now(), 10f, att));
        computed = var.compute(variableStatuses, Instant.now());
        assertEquals(3f, computed.getAsFloat().get(), 0f);
        assertEquals(3, computed.getLastSourceMetrics().size());
    }
    
}
