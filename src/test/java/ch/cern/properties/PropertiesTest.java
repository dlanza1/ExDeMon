package ch.cern.properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.time.Duration;
import java.time.Instant;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import ch.cern.Cache;
import ch.cern.spark.metrics.defined.DefinedMetrics;
import ch.cern.spark.metrics.monitors.Monitors;
import ch.cern.spark.metrics.schema.MetricSchemas;

public class PropertiesTest {
	
	@Before
	public void setUp() throws Exception {
		Properties.initCache(null);
		Monitors.getCache().reset();	
		DefinedMetrics.getCache().reset();
		MetricSchemas.getCache().reset();
	}
    
    @Test
    public void globalParametersNull(){
        Properties prop = new Properties();
        prop.setProperty("prop1", "val1");
        prop.setProperty("prop2.prop1", "val2");
        
        Properties subProp = prop.getSubset("prop2");
        
        Assert.assertEquals(1, subProp.size());
        Assert.assertEquals("val2", subProp.get("prop1"));
    }

	@Test
	public void cacheExpiration() throws Exception{
		Cache<Properties> propertiesCache = Properties.getCache();
		propertiesCache.setExpiration(Duration.ofSeconds(1));
		
		Instant now = Instant.now();
		Properties p1 = propertiesCache.get();
		
		Properties p2 = propertiesCache.get();
		assertSame(p1, p2);
		
		assertTrue(propertiesCache.hasExpired(now.plus(Duration.ofSeconds(2))));
		
		propertiesCache.setExpiration(null);
	}
	
	@Test
	public void propertiesFromDefaultSource() throws Exception{
		Properties props = new Properties();
		props.setProperty("type", "file");
		props.setProperty("path", "src/test/resources/config.properties");
		Properties.getCache().set(props);
		
		assertTrue(Properties.getCache().get().size() > 0);
	}
	
	@Test
	public void getUniqueKeyFields() {
		Properties prop = new Properties();
        prop.setProperty("prop1", "val1");
        prop.setProperty("prop2.prop21", "val2");
        prop.setProperty("prop2.prop22", "val2");
        prop.setProperty("prop2.prop23", "val2");
        prop.setProperty("prop3.prop31", "val2");
        prop.setProperty("prop3.prop32", "val2");
        
        Object[] uniq = prop.getUniqueKeyFields().toArray();
        String[] expectedValue = {"prop2", "prop1", "prop3"};
        Assert.assertArrayEquals(expectedValue, uniq);
	}
	
	@Test
	public void getSubSet() {
		Properties prop = new Properties();
        prop.setProperty("prop1", "val1");
        prop.setProperty("prop2.prop21", "val2");
        prop.setProperty("prop2.prop22", "val2");
        prop.setProperty("prop2.prop23", "val2");
        prop.setProperty("prop3.prop31", "val2");
        prop.setProperty("prop3.prop31.p1", "val2");
        prop.setProperty("prop3.prop31.p2", "val2");
        prop.setProperty("prop3.prop31.p3", "val2");
        prop.setProperty("prop3.prop31.p4", "val2");
        prop.setProperty("prop3.prop32", "val2");
        
        Properties subset = prop.getSubset("prop2");
        assertEquals(3, subset.size());
        
        subset = prop.getSubset("prop3.prop31");
        assertEquals(4, subset.size());
	}
	
}
