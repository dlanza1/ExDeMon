package ch.cern.spark.metrics;

import static org.junit.Assert.*;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.Test;
import org.spark_project.guava.collect.Sets;

public class MetricTest {

    public static Metric build() {
        Map<String, String> ids = new HashMap<>();
        ids.put("key1", "val1");
        ids.put("key2", "val2");
        ids.put("key3", "val3");
        
        Metric metric = new Metric(Instant.ofEpochMilli(1000), 100, ids);
        
        return metric;
    }
    
    @Test
    public void testMetricCreator(){
        Metric metric = Metric(3, 10f, "aaa=12", "bbb=15");
        
        assertEquals(3, metric.getInstant().getEpochSecond());
        assertEquals(10f, metric.getValue(), 0f);
        assertEquals("12", metric.getIDs().get("aaa"));
        assertEquals("15", metric.getIDs().get("bbb"));
    }
    
    public static Metric Metric(int timeInSec, float value, String... ids){
        Map<String, String> idsMap = Sets.newHashSet(ids).stream()
                .map(id -> (String) id)
                .collect(Collectors.toMap(
                        id -> id.toString().substring(0, id.toString().indexOf("=")), 
                        id -> id.substring(id.indexOf("=") + 1)
                    ));
        
        return new Metric(Instant.ofEpochSecond(timeInSec), value, idsMap);
    }

}
