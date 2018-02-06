package ch.cern.spark.metrics.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.util.Optional;

import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.json.JSONObject;
import ch.cern.spark.metrics.value.Value;

public class ValueDescriptorTest {
    
    private ValueDescriptor descriptor = new ValueDescriptor("test");
    
    @Test
    public void shouldParseToString() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty("key", "key");
        props.setProperty("type", "string");
        descriptor.config(props);

        Optional<Value> value = descriptor.extract(new JSONObject("{\"key\": \"12\"}"));
        assertTrue(value.isPresent());
        assertEquals("12", value.get().getAsString().get());

        value = descriptor.extract(new JSONObject("{\"key\": 12}"));
        assertTrue(value.isPresent());
        assertEquals("12", value.get().getAsString().get());
        
        value = descriptor.extract(new JSONObject("{\"key\": true}"));
        assertTrue(value.isPresent());
        assertEquals("true", value.get().getAsString().get());
    }
    
    @Test
    public void shouldParseToNumber() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty("key", "key");
        props.setProperty("type", "numeric");
        descriptor.config(props);

        Optional<Value> value = descriptor.extract(new JSONObject("{\"key\": 12}"));        
        assertTrue(value.isPresent());
        assertEquals(12f, value.get().getAsFloat().get(), 0f);
        
        value = descriptor.extract(new JSONObject("{\"key\": \"12\"}"));        
        assertTrue(value.isPresent());
        assertEquals(12f, value.get().getAsFloat().get(), 0f);
        
        value = descriptor.extract(new JSONObject("{\"key\": \"0012\"}"));        
        assertTrue(value.isPresent());
        assertEquals(12f, value.get().getAsFloat().get(), 0f);
        
        value = descriptor.extract(new JSONObject("{\"key\": \"abc\"}"));        
        assertFalse(value.isPresent());
    }
    
    @Test
    public void shouldParseToBoolean() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty("key", "key");
        props.setProperty("type", "boolean");
        descriptor.config(props);

        Optional<Value> value = descriptor.extract(new JSONObject("{\"key\": true}"));
        assertTrue(value.isPresent());
        assertTrue(value.get().getAsBoolean().get());

        value = descriptor.extract(new JSONObject("{\"key\": false}"));
        assertTrue(value.isPresent());
        assertFalse(value.get().getAsBoolean().get());
        
        value = descriptor.extract(new JSONObject("{\"key\": \"true\"}"));
        assertTrue(value.isPresent());
        assertTrue(value.get().getAsBoolean().get());

        value = descriptor.extract(new JSONObject("{\"key\": \"false\"}"));
        assertTrue(value.isPresent());
        assertFalse(value.get().getAsBoolean().get());
        
        value = descriptor.extract(new JSONObject("{\"key\": \"abs\"}"));
        assertFalse(value.isPresent());
    }
    
    @Test
    public void shouldParseStringWhenAuto() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty("key", "key");
        descriptor.config(props);

        String jsonString = "{\"key\": \"12\"}";

        JSONObject jsonObject = new JSONObject(jsonString);

        Optional<Value> value = descriptor.extract(jsonObject);
        
        assertTrue(value.isPresent());
        assertEquals("12", value.get().getAsString().get());
    }
    
    @Test
    public void shouldParseNumberWhenAuto() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty("key", "key");
        descriptor.config(props);

        String jsonString = "{\"key\": 12}";

        JSONObject jsonObject = new JSONObject(jsonString);

        Optional<Value> value = descriptor.extract(jsonObject);
        
        assertTrue(value.isPresent());
        assertEquals(12f, value.get().getAsFloat().get(), 0f);
    }
    
    @Test
    public void shouldParseBooleanWhenAuto() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty("key", "key");
        descriptor.config(props);

        JSONObject jsonObject = new JSONObject("{\"key\": true}");

        Optional<Value> value = descriptor.extract(jsonObject);
        
        assertTrue(value.isPresent());
        assertTrue(value.get().getAsBoolean().get());
        
        jsonObject = new JSONObject("{\"key\": false}");

        value = descriptor.extract(jsonObject);
        
        assertTrue(value.isPresent());
        assertFalse(value.get().getAsBoolean().get());
    }
    
    @Test
    public void shouldParseStringWithRegexAndAuto() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty("key", "key");
        props.setProperty("regex", "abc(.*)");
        descriptor.config(props);

        Optional<Value> value = descriptor.extract(new JSONObject("{\"key\": \"abcdef\"}"));        
        assertTrue(value.isPresent());
        assertEquals("def", value.get().getAsString().get());
        
        value = descriptor.extract(new JSONObject("{\"key\": \"aBcdef\"}"));        
        assertFalse(value.isPresent());
    }
    
    @Test
    public void shouldParseNumberWithRegexAndAuto() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty("key", "key");
        props.setProperty("regex", "abc(.*)");
        descriptor.config(props);

        Optional<Value> value = descriptor.extract(new JSONObject("{\"key\": \"abc1234\"}"));        
        assertTrue(value.isPresent());
        assertEquals(1234f, value.get().getAsFloat().get(), 0f);
    }
    
    @Test
    public void shouldParseBooleanWithRegexAndAuto() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty("key", "key");
        props.setProperty("regex", "abc(.*)");
        descriptor.config(props);

        Optional<Value> value = descriptor.extract(new JSONObject("{\"key\": \"abctrue\"}"));        
        assertTrue(value.isPresent());
        assertTrue(value.get().getAsBoolean().get());
    }
    
    @Test
    public void shouldParseStringWithRegex() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty("key", "key");
        props.setProperty("type", "string");
        props.setProperty("regex", "abc(.*)");
        descriptor.config(props);

        Optional<Value> value = descriptor.extract(new JSONObject("{\"key\": \"abcdef\"}"));        
        assertTrue(value.isPresent());
        assertEquals("def", value.get().getAsString().get());
        
        value = descriptor.extract(new JSONObject("{\"key\": \"abctrue\"}"));        
        assertTrue(value.isPresent());
        assertEquals("true", value.get().getAsString().get());
        
        value = descriptor.extract(new JSONObject("{\"key\": \"aBcdef\"}"));        
        assertFalse(value.isPresent());
    }
    
    @Test
    public void shouldParseNumberWithRegex() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty("key", "key");
        props.setProperty("type", "numeric");
        props.setProperty("regex", "abc(.*)");
        descriptor.config(props);

        Optional<Value> value = descriptor.extract(new JSONObject("{\"key\": \"abc1234\"}"));        
        assertTrue(value.isPresent());
        assertEquals(1234f, value.get().getAsFloat().get(), 0f);
    }
    
    @Test
    public void shouldParseBooleanWithRegex() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty("key", "key");
        props.setProperty("type", "boolean");
        props.setProperty("regex", "abc(.*)");
        descriptor.config(props);

        Optional<Value> value = descriptor.extract(new JSONObject("{\"key\": \"abctrue\"}"));        
        assertTrue(value.isPresent());
        assertTrue(value.get().getAsBoolean().get());
    }
    
}
