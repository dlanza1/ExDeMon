package ch.cern.properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class PropertiesTest {
	
	@Before
	public void setUp() throws Exception {
		Properties.initCache(null);
		Properties.getCache().reset();
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
        
        Object[] uniq = prop.getIDs().toArray();
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
	
	@Test
	public void fromJSON() {
	    String jsonString = "{\"metrics.schema.perf\":{"
	                + "\"sources\":\"tape_logs\", "
	                + "\"filter.attribute\":\"1234\"}"
	            + "}";

	    JsonObject jsonObject = new JsonParser().parse(jsonString).getAsJsonObject();
        
        Properties props = Properties.from(jsonObject);
        
        assertEquals("tape_logs", props.get("metrics.schema.perf.sources"));
        assertEquals("1234", props.get("metrics.schema.perf.filter.attribute"));
	}
	
}
