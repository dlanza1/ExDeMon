package ch.cern.exdemon.metrics.defined.equation.var.agg;

import static ch.cern.exdemon.metrics.MetricTest.Metric;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.metrics.defined.DefinedMetric;
import ch.cern.exdemon.metrics.defined.equation.var.VariableStatuses;
import ch.cern.properties.Properties;

public class SuccessiveCountAgregationTest {

    @Test
    public void resetCount() throws CloneNotSupportedException {
        DefinedMetric definedMetric = new DefinedMetric("A");
        
        Properties properties = new Properties();
        properties.setProperty("spark.batch.time", "1m");
        properties.setProperty("metrics.filter.attribute.HOSTNAME", "host1");
        properties.setProperty("metrics.groupby", "HOSTNAME");
        properties.setProperty("variables.var.filter.attribute.TYPE", "Read Bytes");
        properties.setProperty("variables.var.aggregate.type", "successive_count");
        definedMetric.config(properties);
        
        Set<String> groupByKeys = new HashSet<>();
        groupByKeys.add("HOSTNAME");
        
        VariableStatuses store = new VariableStatuses();;
        
        Metric metric = Metric(Instant.now(), 10, "HOSTNAME=host1", "TYPE=Read Bytes");
        definedMetric.updateStore(store, metric, groupByKeys);
        assertEquals(1f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
        
        metric = Metric(Instant.now(), 13, "HOSTNAME=host1", "TYPE=Read Bytes");
        definedMetric.updateStore(store, metric, groupByKeys);
        assertEquals(2f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
        
        //Top level filter out, so this one is not taken into account
        metric = Metric(Instant.now(), 13, "HOSTNAME=host2", "TYPE=Read Bytes");
        definedMetric.updateStore(store, metric, groupByKeys);
        assertFalse(definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).isPresent());
        
        metric = Metric(Instant.now(), 13, "HOSTNAME=host1", "TYPE=Read Bytes");
        definedMetric.updateStore(store, metric, groupByKeys);
        assertEquals(3f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
        
        //Should reset
        metric = Metric(Instant.now(), 13, "HOSTNAME=host1", "TYPE=Write Bytes");
        definedMetric.updateStore(store, metric, groupByKeys);
        assertEquals(0f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
        
        metric = Metric(Instant.now(), 13, "HOSTNAME=host1", "TYPE=Write Bytes");
        definedMetric.updateStore(store, metric, groupByKeys);
        assertEquals(0f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
        
        //Restart count
        metric = Metric(Instant.now(), 13, "HOSTNAME=host1", "TYPE=Read Bytes");
        definedMetric.updateStore(store, metric, groupByKeys);
        assertEquals(1f, definedMetric.generateByUpdate(store, metric, new HashMap<String, String>()).get().getValue().getAsFloat().get(), 0.001f);
    }
    
}
