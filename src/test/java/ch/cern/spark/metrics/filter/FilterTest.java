package ch.cern.spark.metrics.filter;

import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.spark.metrics.Metric;

public class FilterTest {
    
    @Test
    public void filterOneID(){
        Filter filter = new Filter();
        filter.addPredicate("K1", "V1");
        
        Map<String, String> ids = new HashMap<>();
        ids.put("K1", "V1");
        Metric metric = new Metric(null, 0, ids );
        Assert.assertTrue(filter.apply(metric));
        
        ids.put("K1", "V2");
        Assert.assertFalse(filter.apply(metric));
    }
    
    @Test
    public void filterSeveralIDs(){
        Filter filter = new Filter();
        filter.addPredicate("K1", "V1");
        filter.addPredicate("K2", "V2");
        
        Map<String, String> ids = new HashMap<>();
        ids.put("K1", "V1");
        ids.put("K2", "V2");
        Metric metric = new Metric(null, 0, ids );
        Assert.assertTrue(filter.apply(metric));
        
        ids.put("K1", "V1");
        ids.put("K1", "V2");
        Assert.assertFalse(filter.apply(metric));
    }
    
    @Test
    public void filterActualValueNull(){
        Filter filter = new Filter();
        filter.addPredicate("K1", "V1");
        
        Map<String, String> ids = new HashMap<>();
        Metric metric = new Metric(null, 0, ids);
        Assert.assertFalse(filter.apply(metric));
    }
    
    @Test
    public void noAttributesFilter(){
        Filter filter = new Filter();
        
        Map<String, String> ids = new HashMap<>();
        ids.put("K1", "V1");
        ids.put("K1", "V2");
        Metric metric = new Metric(null, 0, ids );
        Assert.assertTrue(filter.apply(metric));
    }
    
    @Test
    public void filterRegex(){
        Filter filter = new Filter();
        filter.addPredicate("K1", "regex:V[0-9]");
        filter.addPredicate("K2", "regex:V.*");
        
        Map<String, String> ids = new HashMap<>();
        ids.put("K1", "V5");
        ids.put("K2", "Vfoo");
        Metric metric = new Metric(null, 0, ids );
        Assert.assertTrue(filter.apply(metric));
        
        ids.put("K1", "V2");
        ids.put("K2", "Vyes");
        Assert.assertTrue(filter.apply(metric));
        
        ids.put("K1", "V2");
        ids.put("K2", "Pno");
        Assert.assertFalse(filter.apply(metric));
        
        ids.put("K1", "Vno");
        ids.put("K2", "Vyes");
        Assert.assertFalse(filter.apply(metric));
        
        ids.put("K1", "Vno");
        ids.put("K2", "NO");
        Assert.assertFalse(filter.apply(metric));
    }
    
}
