package ch.cern.spark.metrics;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

public class MetricTest {

    public static Metric build() {
        Map<String, String> ids = new HashMap<>();
        ids.put("key1", "val1");
        ids.put("key2", "val2");
        ids.put("key3", "val3");
        
        Metric metric = new Metric(Instant.ofEpochMilli(1000), 100, ids);
        
        return metric;
    }

}
