package ch.cern.exdemon.metrics;

import static org.junit.Assert.assertEquals;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;
import org.spark_project.guava.collect.Sets;

import ch.cern.exdemon.metrics.value.BooleanValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.metrics.value.Value;

public class MetricTest {

    public static Metric build() {
        Map<String, String> ids = new HashMap<>();
        ids.put("key1", "val1");
        ids.put("key2", "val2");
        ids.put("key3", "val3");
        
        Metric metric = new Metric(Instant.ofEpochMilli(1000), new FloatValue(100), ids);
        
        return metric;
    }
    
    @Test
    public void testMetricCreator(){
        Metric metric = Metric(3, 10f, "aaa=12", "bbb=15");
        
        assertEquals(3, metric.getTimestamp().getEpochSecond());
        assertEquals(10f, metric.getValue().getAsFloat().get(), 0f);
        assertEquals("12", metric.getAttributes().get("aaa"));
        assertEquals("15", metric.getAttributes().get("bbb"));
    }

    public static Metric Metric(int timeInSec, float value, String... ids){
        return Metric(Instant.ofEpochMilli(timeInSec * 1000), new FloatValue(value), ids);
    }
    
    public static Metric Metric(Instant time, float value, String... ids){
        return Metric(time, new FloatValue(value), ids);
    }
    
    public static Metric Metric(int timeInSec, String value, String... ids){
        return Metric(Instant.ofEpochMilli(timeInSec * 1000), new StringValue(value), ids);
    }
    
    public static Metric Metric(int timeInSec, boolean value, String... ids){
        return Metric(Instant.ofEpochMilli(timeInSec * 1000), new BooleanValue(value), ids);
    }
    
    public static Metric Metric(Instant time, Value value, String... ids){
        Map<String, String> idsMap = Sets.newHashSet(ids).stream()
                .map(id -> (String) id)
                .collect(Collectors.toMap(
                        id -> id.toString().substring(0, id.toString().indexOf("=")), 
                        id -> id.substring(id.indexOf("=") + 1)
                    ));
        
        return new Metric(time, value, idsMap);
    }

}
