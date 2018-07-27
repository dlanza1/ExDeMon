package ch.cern.properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import ch.cern.exdemon.components.ConfigurationResult;

public class PropertiesTest {
    
    Properties propertiesSource = new Properties();
    {
        propertiesSource.put("type", "static");
    }
    
    @Test
    public void notUsedProperties() {
        Properties prop = new Properties();
        prop.setProperty("a", "a");
        prop.setProperty("b", "b");
        prop.setProperty("c", "c");

        prop.getProperty("a");
        
        ConfigurationResult confResult = prop.warningsIfNotAllPropertiesUsed();
        assertEquals(2, confResult.getWarnings().size());
        
        prop.getProperty("b");
        prop.getProperty("c");
        
        confResult = prop.warningsIfNotAllPropertiesUsed();
        assertEquals(0, confResult.getWarnings().size());
    }

    @Test
    public void globalParametersNull() {
        Properties prop = new Properties();
        prop.setProperty("prop1", "val1");
        prop.setProperty("prop2.prop1", "val2");

        Properties subProp = prop.getSubset("prop2");

        Assert.assertEquals(1, subProp.size());
        Assert.assertEquals("val2", subProp.get("prop1"));
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
        String[] expectedValue = { "prop2", "prop1", "prop3" };
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
        String jsonString = "{\"metrics.schema.perf\":{" + "\"sources\":\"tape_logs\", "
                + "\"filter.attribute\":\"1234\"}" + "}";

        JsonObject jsonObject = new JsonParser().parse(jsonString).getAsJsonObject();

        Properties props = Properties.from(jsonObject);

        assertEquals("tape_logs", props.get("metrics.schema.perf.sources"));
        assertEquals("1234", props.get("metrics.schema.perf.filter.attribute"));
    }

    @Test
    public void getStaticProperty() {
        Properties sprops = new Properties();
        sprops.setProperty("aa", "aa1");
        
        Properties props = new Properties();
        props.setStaticProperties(sprops);
        
        assertEquals("aa1", props.getProperty("aa"));
        assertEquals("aa1", props.getSubset("").getProperty("aa"));
        assertEquals("aa1", ((Properties)props.clone()).getProperty("aa"));
    }
    
    @Test
    public void getReferencedProperty() {
        Properties props = new Properties();
        props.setProperty("aa", "aa1");
        props.setProperty("bb", "@aa");
        
        assertEquals("aa1", props.getProperty("bb"));
    }
    
    @Test
    public void getReferencedStaticProperty() {
        Properties sprops = new Properties();
        sprops.setProperty("aa", "aa1");
        
        Properties props = new Properties();
        props.setStaticProperties(sprops);
        props.setProperty("aa.bb", "@aa");
        
        assertEquals("aa1", props.getProperty("aa.bb"));
        assertEquals("aa1", props.getSubset("aa").getProperty("bb"));
        assertEquals("aa1", ((Properties)props.clone()).getSubset("aa").getProperty("bb"));
    }

    @Test
    public void fromClasspath() throws IOException {
        Properties props = Properties.fromFile("classpath:/config.properties");

        assertTrue(props.size() > 0);
    }

}
