package ch.cern.exdemon.metrics.schema;

import static ch.cern.exdemon.metrics.schema.MetricSchema.ATTRIBUTES_PARAM;
import static ch.cern.exdemon.metrics.schema.MetricSchema.FILTER_PARAM;
import static ch.cern.exdemon.metrics.schema.MetricSchema.SOURCES_PARAM;
import static ch.cern.exdemon.metrics.schema.MetricSchema.TIMESTAMP_PARAM;
import static ch.cern.exdemon.metrics.schema.MetricSchema.VALUES_PARAM;
import static ch.cern.exdemon.metrics.schema.TimestampDescriptor.FORMAT_PARAM;
import static ch.cern.exdemon.metrics.schema.TimestampDescriptor.KEY_PARAM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.util.Iterator;
import java.util.concurrent.atomic.LongAccumulator;

import org.junit.Before;
import org.junit.Test;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.json.JSON;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.utils.TimeUtils;

public class MetricSchemaTest {

    private MetricSchema schema;

    @Before
    public void setUp() {
        schema = new MetricSchema("test" + (int)(Math.random() * 10000));
    }

//    @Test
    public void performance() throws InterruptedException {
        if(!ManagementFactory.getThreadMXBean().isThreadCpuTimeSupported())
            return;
        
        LongAccumulator threadCpuTime = new LongAccumulator((a, b) -> a + b, 0);
        
        Thread thread = new Thread() {
            @Override
            public void run() {
                try {
                    for (int i = 0; i < 10000000; i++)
                        severalAttributesWithRegexAndReplacementInAlias();
                } catch (Exception e) {}
                
                threadCpuTime.accumulate(ManagementFactory.getThreadMXBean().getThreadCpuTime(Thread.currentThread().getId()));
                
                super.run();
            }
        };
        thread.start();
        thread.join();
        
        System.out.println("Took " + TimeUtils.toString(Duration.ofNanos(threadCpuTime.get())) + " ns");
    }
    
    @Test
    public void shouldFilter() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(ATTRIBUTES_PARAM + ".version.key", "metadata.version");
        props.setProperty("value.type_prefix.key", "metadata.type_prefix");

        props.setProperty(FILTER_PARAM + ".attribute.version", "001");

        schema.config(props);

        String jsonString = "{\"metadata\":{" + "\"type_prefix\":\"raw\"," + "\"version\":\"001\"," + "\"time\": "
                + Instant.now().toEpochMilli() + " }}";
        JSON jsonObject = new JSON(jsonString);
        Iterator<Metric> metrics = schema.call(jsonObject).iterator();
        assertTrue(metrics.hasNext());

        jsonString = "{\"metadata\":{" + "\"type_prefix\":\"raw\"," + "\"version\":\"002\"," + "\"time\": "
                + Instant.now().toEpochMilli() + " }}";
        jsonObject = new JSON(jsonString);
        metrics = schema.call(jsonObject).iterator();
        assertFalse(metrics.hasNext());
    }
    
    @Test
    public void warningWhenFilterWithNonExistingAttribute() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(ATTRIBUTES_PARAM + ".version.key", "metadata.version");
        props.setProperty("value.type_prefix.key", "metadata.type_prefix");

        props.setProperty(FILTER_PARAM + ".attribute.version", "001");
        props.setProperty(FILTER_PARAM + ".attribute.noExist1", "001");
        props.setProperty(FILTER_PARAM + ".attribute.noExist2", "001");

        assertEquals("filtering with attributes [noExist2, noExist1] not configured in the schema", schema.config(props).getWarnings().get(0).getMessage());
    }
    
    @Test
    public void shouldGenerateSeveralMetricsWithSeveralValues() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(VALUES_PARAM + ".type_prefix.key", "metadata.type_prefix");
        props.setProperty(VALUES_PARAM + ".type_prefix.regex", "r(.)w");
        props.setProperty(VALUES_PARAM + ".version.key",     "metadata.version");
        props.setProperty(VALUES_PARAM + ".version.type",    "numeric");

        schema.config(props);

        String jsonString = "{\"metadata\":{" 
                                + "\"type_prefix\":\"raw\"," 
                                + "\"version\":1," 
                                + "\"time\": " + Instant.now().toEpochMilli() + " }}";
        JSON jsonObject = new JSON(jsonString);
        Iterator<Metric> metrics = schema.call(jsonObject).iterator();
        assertTrue(metrics.hasNext());
        Metric metric = metrics.next();
        assertEquals("a", metric.getValue().getAsString().get());
        
        assertTrue(metrics.hasNext());
        metric = metrics.next();
        assertEquals(1f, metric.getValue().getAsFloat().get(), 0f);
    }

    @Test
    public void attributeNullIfNoJsonPrimitive() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_PARAM + "." + KEY_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_PARAM + "." + FORMAT_PARAM, "epoch-ms");
        props.setProperty("value.pending_slots.key", "data.payload.WMBS_INFO.thresholds.pending_slots");

        String att_name_no_primitive = "data.payload.WMBS_INFO.thresholds";
        props.setProperty(ATTRIBUTES_PARAM + ".data.key", att_name_no_primitive);

        String jsonString = "{\"metadata\":{" + "\"timestamp\":1509520209883" + "},\"data\":{" + "\"payload\":{"
                + "\"WMBS_INFO\":{" + "\"thresholds\":{" + "\"pending_slots\":2111.89}," + "\"thresholdsGQ2LQ\":2111.0}"
                + "}}}";
        JSON jsonObject = new JSON(jsonString);

        schema.config(props);

        Iterator<Metric> metrics = schema.call(jsonObject).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertNull(metric.getAttributes().get(att_name_no_primitive));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void attributeStringFromNumber() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_PARAM + "." + KEY_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_PARAM + "." + FORMAT_PARAM, "epoch-ms");
        props.setProperty("value.pending_slots.key", "data.payload.WMBS_INFO.thresholds.pending_slots");

        props.setProperty(ATTRIBUTES_PARAM + ".thresholdsGQ2LQ", "data.payload.WMBS_INFO.thresholdsGQ2LQ");
        schema.config(props);

        String jsonString = "{\"metadata\":{" + "\"timestamp\":1509520209883" + "},\"data\":{" + "\"payload\":{"
                + "\"WMBS_INFO\":{" + "\"thresholds\":{" + "\"pending_slots\":2111.89}," + "\"thresholdsGQ2LQ\":2111.0}"
                + "}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = schema.call(jsonObject).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals("2111.0", metric.getAttributes().get("thresholdsGQ2LQ"));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void severalAttributesWithRegex() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_PARAM + "." + KEY_PARAM, "metadata.timestamp");
        props.setProperty("value.test.key", "test");

        props.setProperty(ATTRIBUTES_PARAM + ".error_+", "a.b.error-(.*)");
        schema.config(props);

        String jsonString = "{\"a\":{\"b\":{\"error-1\": 1, \"error-abcd\": 2}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = schema.call(jsonObject).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals("1", metric.getAttributes().get("error_1"));
        assertEquals("2", metric.getAttributes().get("error_abcd"));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void severalAttributesWithRegexWithPlus() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_PARAM + "." + KEY_PARAM, "metadata.timestamp");
        props.setProperty("value.test.key", "test");

        props.setProperty(ATTRIBUTES_PARAM + ".error_+", "a.b.error-([0-9]+)");
        schema.config(props);

        String jsonString = "{\"a\":{\"b\":{\"error-12\": 1, \"error-23\": 2}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = schema.call(jsonObject).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals("1", metric.getAttributes().get("error_12"));
        assertEquals("2", metric.getAttributes().get("error_23"));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void severalAttributesWithRegexAndReplacementInAlias() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty("value.a.key", "a.b.error-1");

        props.setProperty(ATTRIBUTES_PARAM + ".error_+", "a.b.error-(.*)");
        schema.config(props);

        String jsonString = "{\"a\":{\"b\":{\"error-1\": 1, \"error-abcd\": 2}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = schema.call(jsonObject).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals("1", metric.getAttributes().get("error_1"));
        assertEquals("2", metric.getAttributes().get("error_abcd"));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void severalAttributesWithRegexAndReplacementInAliasButNoGroup()
            throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_PARAM + "." + KEY_PARAM, "metadata.timestamp");
        props.setProperty("value.t.key", "test");

        props.setProperty(ATTRIBUTES_PARAM + ".error_+", "a.b.error-.*");
        ConfigurationResult confResult = schema.config(props);

        assertTrue(confResult.getErrors().get(0).getMessage().contains("no regular expression with one group"));
    }

    @Test
    public void attributesWithFixedAttributes() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_PARAM + "." + KEY_PARAM, "metadata.timestamp");
        props.setProperty("value.test.key", "test");

        props.setProperty(ATTRIBUTES_PARAM + ".puppet_environment_DEPRECATED", "#qa_deprecated");
        props.setProperty(ATTRIBUTES_PARAM + ".puppet_environment.value", "qa");
        schema.config(props);

        String jsonString = "{\"a\":{\"b\":{\"error-1\": 1, \"error-abcd\": 2}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = schema.call(jsonObject).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals("qa_deprecated", metric.getAttributes().get("puppet_environment_DEPRECATED"));
        assertEquals("qa", metric.getAttributes().get("puppet_environment"));

        assertFalse(metrics.hasNext());
    }
    
    @Test
    public void attributesWithRegex() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty("value.test.key", "a.b.error");

        props.setProperty(ATTRIBUTES_PARAM + ".error.key", "a.b.error");
        props.setProperty(ATTRIBUTES_PARAM + ".error.regex", "ERROR (.*) PARAMS .*");
        props.setProperty(ATTRIBUTES_PARAM + ".params.key", "a.b.error");
        props.setProperty(ATTRIBUTES_PARAM + ".params.regex", ".* PARAMS (.*)");
        schema.config(props);
        
        String jsonString = "{\"a\":{\"b\":{\"error\": \"ERROR error message PARAMS paramValues\"}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = schema.call(jsonObject).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals("error message", metric.getAttributes().get("error"));
        assertEquals("paramValues", metric.getAttributes().get("params"));

        assertFalse(metrics.hasNext());
    }
    
    @Test
    public void shouldNotGenerateSameExceptions() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_PARAM + "." + KEY_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_PARAM + "." + FORMAT_PARAM, "yyyy MM dd");
        props.setProperty("value.keys.data.payload.WMBS_INFO.thresholds.pending_slots",
                "data.payload.WMBS_INFO.thresholds.pending_slots");
        schema.config(props);

        String jsonString = "{\"metadata\":{" + "\"timestamp\":\"10:20:12\"" + "},\"data\":{" + "\"payload\":{"
                + "\"WMBS_INFO\":{" + "\"thresholds\":{" + "\"pending_slots\":2111.89}," + "\"thresholdsGQ2LQ\":2111.0}"
                + "}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = schema.call(jsonObject).iterator();

        assertTrue(metrics.hasNext());
        Metric metric = metrics.next();
        assertTrue(metric.getValue().getAsException().isPresent());
        assertFalse(metrics.hasNext());
        
        metrics = schema.call(jsonObject).iterator();
        assertFalse(metrics.hasNext());
        
        metrics = schema.call(jsonObject).iterator();
        assertFalse(metrics.hasNext());
    }

    @Test
    public void shouldParseWithMissingAttributes() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_PARAM + "." + KEY_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_PARAM + "." + FORMAT_PARAM, "epoch-ms");

        props.setProperty("value.running_slots.key", "data.payload.WMBS_INFO.thresholds.running_slots");

        props.setProperty(ATTRIBUTES_PARAM + ".site_name", "data.payload.site_name");
        props.setProperty(ATTRIBUTES_PARAM + ".agent_url", "data.payload.agent_url");
        props.setProperty(ATTRIBUTES_PARAM + ".type.meta", "metadata.type");
        props.setProperty(ATTRIBUTES_PARAM + ".version", "metadata.version");
        schema.config(props);

        String jsonString = "{\"metadata\":{" 
                                + "\"timestamp\":1509520209883}," 
                             + "\"data\":{" 
                                + "\"payload\":{"
                                    + "\"site_name\":\"T2_UK_London_Brunel\"," 
                                    + "\"timestamp\":1509519908," 
                                    + "\"LocalWQ_INFO\":{},"
                                    + "\"WMBS_INFO\":{" 
                                        + "\"thresholds\":{" 
                                            + "\"state\":\"Normal\"," 
                                            + "\"running_slots\":2815,"
                                            + "\"pending_slots\":2111.89}," 
                                        + "\"thresholdsGQ2LQ\":2111.0},"
                                    + "\"agent_url\":\"vocms0258.cern.ch\"," 
                                    + "\"type\":\"site_info\"}" 
                                 + "}" 
                              + "}";

        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = schema.call(jsonObject).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals(1509520209883l, metric.getTimestamp().toEpochMilli());
        assertEquals(2815f, metric.getValue().getAsFloat().get(), 0f);
        assertEquals(4, metric.getAttributes().size());
        assertTrue("test", metric.getAttributes().get("$schema").startsWith("test"));
        assertEquals("running_slots", metric.getAttributes().get("$value"));
        assertEquals("T2_UK_London_Brunel", metric.getAttributes().get("site_name"));
        assertEquals("vocms0258.cern.ch", metric.getAttributes().get("agent_url"));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void shouldThrowAnExcpetionIfValueNotConfigured() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_PARAM + "." + KEY_PARAM, "metadata.timestamp");
        // props.setProperty("value.v.key", "value");

        assertEquals("value: must be configured", schema.config(props).getErrors().get(0).toString());
    }
    
    @Test
    public void shouldGenerateExceptionMetricWhenExceptionOcurred() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_PARAM + "." + KEY_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_PARAM + "." + FORMAT_PARAM, "epoch-ms");
        props.setProperty("attributes.site_name", "data.payload.site_name");
        props.setProperty("attributes.agent_url", "data.payload.agent_url");
        
        props.setProperty("value.running_slots.key", "data.payload.WMBS_INFO.thresholds.running_slots");
        props.setProperty("value.pending_slots.key", "data.payload.WMBS_INFO.thresholds.pending_slots");
        schema.config(props);

        String jsonString = "{\"metadata\":{"
                // + "\"timestamp\":1509520209883"
                + "},\"data\":{" + "\"payload\":{" + "\"site_name\":\"T2_UK_London_Brunel\","
                + "\"timestamp\":1509519908," + "\"LocalWQ_INFO\":{}," + "\"WMBS_INFO\":{" + "\"thresholds\":{"
                + "\"state\":\"Normal\"," + "\"running_slots\":2815," + "\"pending_slots\":2111.89},"
                + "\"thresholdsGQ2LQ\":2111.0}," + "\"agent_url\":\"vocms0258.cern.ch\"," + "\"type\":\"site_info\"}"
                + "}" + "}";

        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = schema.call(jsonObject).iterator();

        assertTrue(metrics.hasNext());
        Metric metric = metrics.next();
        assertNotNull(metric.getTimestamp());
        assertTrue(metric.getValue().getAsException().isPresent());
        assertEquals(4, metric.getAttributes().size());
        assertTrue("test", metric.getAttributes().get("$schema").startsWith("test"));
        assertEquals("running_slots", metric.getAttributes().get("$value"));
        assertEquals("T2_UK_London_Brunel", metric.getAttributes().get("site_name"));
        assertEquals("vocms0258.cern.ch", metric.getAttributes().get("agent_url"));

        assertTrue(metrics.hasNext());
        metric = metrics.next();
        assertNotNull(metric.getTimestamp());
        assertTrue(metric.getValue().getAsException().isPresent());
        assertEquals(4, metric.getAttributes().size());
        assertTrue("test", metric.getAttributes().get("$schema").startsWith("test"));
        assertEquals("pending_slots", metric.getAttributes().get("$value"));
        assertEquals("T2_UK_London_Brunel", metric.getAttributes().get("site_name"));
        assertEquals("vocms0258.cern.ch", metric.getAttributes().get("agent_url"));

        assertFalse(metrics.hasNext());
    }

}
