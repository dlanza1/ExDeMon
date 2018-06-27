package ch.cern.properties.source;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

import ch.cern.components.RegisterComponentType;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.schema.MetricSchemas;

@RegisterComponentType("test")
public class PropertiesSourceTest extends PropertiesSource {

    private static final long serialVersionUID = 79323351398301182L;

    @Override
    public Properties loadAll() {
        Properties properties = new Properties();

        properties.setProperty("metrics.source.kafka-prod.type", "not-valid-already-declared");
        properties.setProperty("results.sink.type", "not-valid-already-declared");

        properties.setProperty("key1", "val1");
        properties.setProperty("key2", "val2");

        properties.setProperty(MetricSchemas.PARAM + ".owner1_env1_id1.something", "valid");
        properties.setProperty(MetricSchemas.PARAM + ".owner1$env1$id2.something", "not_valid_id");
        properties.setProperty(MetricSchemas.PARAM + ".owner1_env1_id3.something", "valid");
        properties.setProperty(MetricSchemas.PARAM + ".owner1_env2_id3.something1", "valid");
        properties.setProperty(MetricSchemas.PARAM + ".owner1_env2_id3.something2", "valid");
        properties.setProperty(MetricSchemas.PARAM + ".owner1_env3_id3.something2", "valid");

        return properties;
    }
    
    @Test
    public void loadAllProperties() throws Exception {
        PropertiesSourceTest source = new PropertiesSourceTest();
        Properties sourceProperties = new Properties();
        source.config(sourceProperties);

        Properties loadedProps = source.loadAll();

        assertEquals(10, loadedProps.size());
    }

    @Test
    public void loadAndFilterDefault() throws Exception {
        PropertiesSourceTest source = new PropertiesSourceTest();
        Properties sourceProperties = new Properties();
        source.config(sourceProperties);

        Properties loadedProps = source.load();

        assertEquals(5, loadedProps.size());
    }

    @Test
    public void loadAndFilter() throws Exception {
        PropertiesSourceTest source = new PropertiesSourceTest();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("id.filters.env1", "owner._env1_.*");
        sourceProperties.setProperty("id.filters.env2", "owner._env2_.*");
        source.config(sourceProperties);

        Properties loadedProps = source.load();

        assertEquals(4, loadedProps.size());
    }
    
    @Test
    public void loadStaticProperties() throws Exception {
        PropertiesSourceTest source = new PropertiesSourceTest();
        Properties sourceProperties = new Properties();
        sourceProperties.setProperty("static.a.b.c", "abc");
        sourceProperties.setProperty("static.b.c.d", "bcd");
        source.config(sourceProperties);

        Properties loadedProps = source.load();

        assertEquals("abc", loadedProps.getProperty("a.b.c"));
        assertEquals("bcd", loadedProps.getProperty("b.c.d"));
    }

}
