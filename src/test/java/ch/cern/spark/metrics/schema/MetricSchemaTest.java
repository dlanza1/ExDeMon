package ch.cern.spark.metrics.schema;

import static ch.cern.spark.metrics.schema.MetricSchema.ATTRIBUTES_PARAM;
import static ch.cern.spark.metrics.schema.MetricSchema.FILTER_PARAM;
import static ch.cern.spark.metrics.schema.MetricSchema.SOURCES_PARAM;
import static ch.cern.spark.metrics.schema.MetricSchema.TIMESTAMP_ATTRIBUTE_PARAM;
import static ch.cern.spark.metrics.schema.MetricSchema.TIMESTAMP_FORMAT_PARAM;
import static ch.cern.spark.metrics.schema.MetricSchema.VALUES_PARAM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.text.ParseException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.LongAccumulator;

import org.junit.Before;
import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.json.JSON;
import ch.cern.spark.metrics.Metric;
import ch.cern.utils.TimeUtils;

public class MetricSchemaTest {

    private MetricSchema parser;

    @Before
    public void setUp() {
        parser = new MetricSchema("test" + (int)(Math.random() * 10000));
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
        props.setProperty(ATTRIBUTES_PARAM + ".version", "metadata.version");
        props.setProperty("value.keys.metadata.type_prefix", "metadata.type_prefix");

        props.setProperty(FILTER_PARAM + ".attribute.version", "001");

        parser.config(props);

        String jsonString = "{\"metadata\":{" + "\"type_prefix\":\"raw\"," + "\"version\":\"001\"," + "\"time\": "
                + Instant.now().toEpochMilli() + " }}";
        JSON jsonObject = new JSON(jsonString);
        Iterator<Metric> metrics = parser.call(jsonObject.toString().toString()).iterator();
        assertTrue(metrics.hasNext());

        jsonString = "{\"metadata\":{" + "\"type_prefix\":\"raw\"," + "\"version\":\"002\"," + "\"time\": "
                + Instant.now().toEpochMilli() + " }}";
        jsonObject = new JSON(jsonString);
        metrics = parser.call(jsonObject.toString()).iterator();
        assertFalse(metrics.hasNext());
    }
    
    @Test
    public void shouldGenerateSeveralMetricsWithSeveralValues() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(VALUES_PARAM + ".type_prefix.key", "metadata.type_prefix");
        props.setProperty(VALUES_PARAM + ".type_prefix.regex", "r(.)w");
        props.setProperty(VALUES_PARAM + ".version.key",     "metadata.version");
        props.setProperty(VALUES_PARAM + ".version.type",    "numeric");

        parser.config(props);

        String jsonString = "{\"metadata\":{" 
                                + "\"type_prefix\":\"raw\"," 
                                + "\"version\":1," 
                                + "\"time\": " + Instant.now().toEpochMilli() + " }}";
        JSON jsonObject = new JSON(jsonString);
        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();
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
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
        props.setProperty("value.keys", "data.payload.WMBS_INFO.thresholds.pending_slots");

        String att_name_no_primitive = "data.payload.WMBS_INFO.thresholds";
        props.setProperty(ATTRIBUTES_PARAM, att_name_no_primitive);

        String jsonString = "{\"metadata\":{" + "\"timestamp\":1509520209883" + "},\"data\":{" + "\"payload\":{"
                + "\"WMBS_INFO\":{" + "\"thresholds\":{" + "\"pending_slots\":2111.89}," + "\"thresholdsGQ2LQ\":2111.0}"
                + "}}}";
        JSON jsonObject = new JSON(jsonString);

        parser.config(props);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertNull(metric.getAttributes().get(att_name_no_primitive));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void attributeStringFromNumber() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
        props.setProperty("value.keys.data.payload.WMBS_INFO.thresholds.pending_slots",
                "data.payload.WMBS_INFO.thresholds.pending_slots");

        String att_name_no_primitive = "data.payload.WMBS_INFO.thresholdsGQ2LQ";
        props.setProperty(ATTRIBUTES_PARAM, att_name_no_primitive);
        parser.config(props);

        String jsonString = "{\"metadata\":{" + "\"timestamp\":1509520209883" + "},\"data\":{" + "\"payload\":{"
                + "\"WMBS_INFO\":{" + "\"thresholds\":{" + "\"pending_slots\":2111.89}," + "\"thresholdsGQ2LQ\":2111.0}"
                + "}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals("2111.0", metric.getAttributes().get(att_name_no_primitive));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void severalAttributesWithRegex() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty("value.keys.test", "test");

        props.setProperty(ATTRIBUTES_PARAM, "a.b.error-.*");
        parser.config(props);

        String jsonString = "{\"a\":{\"b\":{\"error-1\": 1, \"error-abcd\": 2}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals("1", metric.getAttributes().get("a.b.error-1"));
        assertEquals("2", metric.getAttributes().get("a.b.error-abcd"));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void severalAttributesWithRegexWithPlus() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty("value.keys.test", "test");

        props.setProperty(ATTRIBUTES_PARAM, "a.b.error-[0-9]+");
        parser.config(props);

        String jsonString = "{\"a\":{\"b\":{\"error-12\": 1, \"error-23\": 2}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals("1", metric.getAttributes().get("a.b.error-12"));
        assertEquals("2", metric.getAttributes().get("a.b.error-23"));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void severalAttributesWithRegexAndReplacementInAlias() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty("value.a.key", "a.b.error-1");

        props.setProperty(ATTRIBUTES_PARAM + ".error_+", "a.b.error-(.*)");
        parser.config(props);

        String jsonString = "{\"a\":{\"b\":{\"error-1\": 1, \"error-abcd\": 2}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

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
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty("value.keys.t", "test");

        props.setProperty(ATTRIBUTES_PARAM + ".error_+", "a.b.error-.*");
        parser.config(props);

        String jsonString = "{\"a\":{\"b\":{\"error-1\": 1, \"error-abcd\": 2}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals("1", metric.getAttributes().get("a.b.error-1"));
        assertEquals("2", metric.getAttributes().get("a.b.error-abcd"));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void attributesWithFixedAttributes() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty("value.keys.test", "test");

        props.setProperty(ATTRIBUTES_PARAM + ".puppet_environment", "#qa");
        props.setProperty(ATTRIBUTES_PARAM + ".error1", "a.b.error-1");
        parser.config(props);

        String jsonString = "{\"a\":{\"b\":{\"error-1\": 1, \"error-abcd\": 2}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals("qa", metric.getAttributes().get("puppet_environment"));
        assertEquals("1", metric.getAttributes().get("error1"));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void shouldParseTimestampToCurrentTimeIfNotConfigured() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty("value.keys.data", "data");
        parser.config(props);

        JSON jsonObject = new JSON("{\"data\": 1}");
        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();
        assertEquals(Instant.now().toEpochMilli(), metrics.next().getTimestamp().toEpochMilli(), 10);

        assertFalse(metrics.hasNext());
    }

    @Test
    public void shouldParseTimestampWithFormatInAuto() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_FORMAT_PARAM, "auto");
        props.setProperty("value.keys.data", "data");
        parser.config(props);

        JSON jsonObject = new JSON("{\"metadata\":{\"timestamp\":1509520209883 }, \"data\": 1}");
        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();
        assertEquals(1509520209883l, metrics.next().getTimestamp().toEpochMilli());

        jsonObject = new JSON("{\"metadata\":{\"timestamp\":1509520209 }, \"data\": 1}");
        metrics = parser.call(jsonObject.toString()).iterator();
        assertEquals(1509520209000l, metrics.next().getTimestamp().toEpochMilli());

        jsonObject = new JSON("{\"metadata\":{\"timestamp\":\"2017-11-01T08:10:09+0100\" }, \"data\": 1}");
        metrics = parser.call(jsonObject.toString()).iterator();
        assertEquals(1509520209000l, metrics.next().getTimestamp().toEpochMilli());

        jsonObject = new JSON("{\"metadata\":{\"timestamp\":\"2017-11-01 08:10:09+0100\" }, \"data\": 1}");
        metrics = parser.call(jsonObject.toString()).iterator();
        assertEquals(1509520209000l, metrics.next().getTimestamp().toEpochMilli());
        
        jsonObject = new JSON("{\"metadata\":{\"timestamp\":\"2018-02-12T11:51:13.963Z\" }, \"data\": 1}");
        metrics = parser.call(jsonObject.toString()).iterator();
        assertEquals(1518436273963l, metrics.next().getTimestamp().toEpochMilli());

        assertFalse(metrics.hasNext());
    }

    @Test
    public void shouldParseTimestampWithFormatInMs() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
        props.setProperty("value.keys.data.payload.WMBS_INFO.thresholds.pending_slots",
                "data.payload.WMBS_INFO.thresholds.pending_slots");
        parser.config(props);

        String jsonString = "{\"metadata\":{" + "\"timestamp\":1509520209883" + "},\"data\":{" + "\"payload\":{"
                + "\"WMBS_INFO\":{" + "\"thresholds\":{" + "\"pending_slots\":2111.89}," + "\"thresholdsGQ2LQ\":2111.0}"
                + "}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals(1509520209883l, metric.getTimestamp().toEpochMilli());

        assertFalse(metrics.hasNext());
    }

    @Test
    public void shouldParseTimestampWithFormatInS() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-s");
        props.setProperty("value.keys.data.payload.WMBS_INFO.thresholds.pending_slots",
                "data.payload.WMBS_INFO.thresholds.pending_slots");
        parser.config(props);

        String jsonString = "{\"metadata\":{" + "\"timestamp\":1509520209" + "},\"data\":{" + "\"payload\":{"
                + "\"WMBS_INFO\":{" + "\"thresholds\":{" + "\"pending_slots\":2111.89}," + "\"thresholdsGQ2LQ\":2111.0}"
                + "}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals(1509520209000l, metric.getTimestamp().toEpochMilli());

        assertFalse(metrics.hasNext());
    }

    @Test
    public void shouldParseDateTimeTimestampWithFormatInDateFormat() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_FORMAT_PARAM, "yyyy-MM-dd HH:mm:ssZ");
        props.setProperty("value.keys.data.payload.WMBS_INFO.thresholds.pending_slots",
                "data.payload.WMBS_INFO.thresholds.pending_slots");
        parser.config(props);

        String jsonString = "{\"metadata\":{" + "\"timestamp\":\"2017-10-20 02:00:12+0000\"" + "},\"data\":{"
                + "\"payload\":{" + "\"WMBS_INFO\":{" + "\"thresholds\":{" + "\"pending_slots\":2111.89},"
                + "\"thresholdsGQ2LQ\":2111.0}" + "}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        // Value should be --> Fri Oct 20 2017 00:00:12 UTC
        Instant timestamp = Instant.parse("2017-10-20T02:00:12.000Z");
        assertEquals(timestamp.toEpochMilli(), metric.getTimestamp().toEpochMilli());

        assertFalse(metrics.hasNext());
    }

    @Test
    public void shouldParseDateTimeTimestampWithFormatInDateFormatWithoutTimeZone()
            throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_FORMAT_PARAM, "yyyy-MM-dd HH:mm:ss");
        props.setProperty("value.keys.data.payload.WMBS_INFO.thresholds.pending_slots",
                "data.payload.WMBS_INFO.thresholds.pending_slots");
        parser.config(props);

        String jsonString = "{\"metadata\":{" + "\"timestamp\":\"2017-12-20 02:00:12\"" + "},\"data\":{"
                + "\"payload\":{" + "\"WMBS_INFO\":{" + "\"thresholds\":{" + "\"pending_slots\":2111.89},"
                + "\"thresholdsGQ2LQ\":2111.0}" + "}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();

        DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").toFormatter()
                .withZone(ZoneId.systemDefault());

        // Value should be --> Fri Oct 20 2017 00:00:12 UTC
        Instant timestamp = Instant.from(formatter.parse("2017-12-20 02:00:12"));
        assertEquals(timestamp, metric.getTimestamp());

        assertFalse(metrics.hasNext());
    }

    @Test
    public void shouldParseDateTimestampWithFormatInDateFormat() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_FORMAT_PARAM, "yyyy MM dd");
        props.setProperty("value.keys.data.payload.WMBS_INFO.thresholds.pending_slots",
                "data.payload.WMBS_INFO.thresholds.pending_slots");
        parser.config(props);

        String jsonString = "{\"metadata\":{" + "\"timestamp\":\"2017 12 20\"" + "},\"data\":{" + "\"payload\":{"
                + "\"WMBS_INFO\":{" + "\"thresholds\":{" + "\"pending_slots\":2111.89}," + "\"thresholdsGQ2LQ\":2111.0}"
                + "}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();

        DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy MM dd HH:mm:ss").toFormatter()
                .withZone(ZoneId.systemDefault());

        Instant timestamp = Instant.from(formatter.parse("2017 12 20 00:00:00"));
        assertEquals(timestamp, metric.getTimestamp());

        assertFalse(metrics.hasNext());
    }
    
    @Test
    public void shouldNotGenerateSameExceptions() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_FORMAT_PARAM, "yyyy MM dd");
        props.setProperty("value.keys.data.payload.WMBS_INFO.thresholds.pending_slots",
                "data.payload.WMBS_INFO.thresholds.pending_slots");
        parser.config(props);

        String jsonString = "{\"metadata\":{" + "\"timestamp\":\"10:20:12\"" + "},\"data\":{" + "\"payload\":{"
                + "\"WMBS_INFO\":{" + "\"thresholds\":{" + "\"pending_slots\":2111.89}," + "\"thresholdsGQ2LQ\":2111.0}"
                + "}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        assertTrue(metrics.hasNext());
        Metric metric = metrics.next();
        assertTrue(metric.getValue().getAsException().isPresent());
        assertFalse(metrics.hasNext());
        
        metrics = parser.call(jsonObject.toString()).iterator();
        assertFalse(metrics.hasNext());
        
        metrics = parser.call(jsonObject.toString()).iterator();
        assertFalse(metrics.hasNext());
    }

    @Test
    public void shouldGenerateExceptionMetricIfTimestampHasWrongFormatInDateFormat()
            throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_FORMAT_PARAM, "yyyy MM dd");
        props.setProperty("value.keys.data.payload.WMBS_INFO.thresholds.pending_slots",
                "data.payload.WMBS_INFO.thresholds.pending_slots");
        parser.config(props);

        String jsonString = "{\"metadata\":{" + "\"timestamp\":\"10:20:12\"" + "},\"data\":{" + "\"payload\":{"
                + "\"WMBS_INFO\":{" + "\"thresholds\":{" + "\"pending_slots\":2111.89}," + "\"thresholdsGQ2LQ\":2111.0}"
                + "}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        assertTrue(metrics.hasNext());
        Metric metric = metrics.next();
        assertTrue(metric.getValue().getAsException().isPresent());

        assertFalse(metrics.hasNext());
    }

    @Test
    public void shouldGenerateExceptionMetricIfTimestampHasWrongFormatInEpoch()
            throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
        props.setProperty("value.keys.data.payload.WMBS_INFO.thresholds.pending_slots",
                "data.payload.WMBS_INFO.thresholds.pending_slots");
        parser.config(props);

        String jsonString = "{\"metadata\":{" + "\"timestamp\":\"10:20:12\"" + "},\"data\":{" + "\"payload\":{"
                + "\"WMBS_INFO\":{" + "\"thresholds\":{" + "\"pending_slots\":2111.89}," + "\"thresholdsGQ2LQ\":2111.0}"
                + "}}}";
        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        assertTrue(metrics.hasNext());
        Metric metric = metrics.next();
        assertNotNull(metric.getTimestamp());
        assertTrue(metric.getValue().getAsException().isPresent());
        assertEquals(3, metric.getAttributes().size());
        assertTrue("test", metric.getAttributes().get("$schema").startsWith("test"));
        assertEquals("data.payload.WMBS_INFO.thresholds.pending_slots", metric.getAttributes().get("$value"));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void shouldParseWithMissingAttributes() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");

        props.setProperty("value.keys.data.payload.WMBS_INFO.thresholds.running_slots",
                "data.payload.WMBS_INFO.thresholds.running_slots");

        props.setProperty(ATTRIBUTES_PARAM, "data.payload.site_name data.payload.agent_url");
        props.setProperty(ATTRIBUTES_PARAM + ".type.meta", "metadata.type");
        props.setProperty(ATTRIBUTES_PARAM + ".version", "metadata.version");
        parser.config(props);

        String jsonString = "{\"metadata\":{" + "\"timestamp\":1509520209883}," + "\"data\":{" + "\"payload\":{"
                + "\"site_name\":\"T2_UK_London_Brunel\"," + "\"timestamp\":1509519908," + "\"LocalWQ_INFO\":{},"
                + "\"WMBS_INFO\":{" + "\"thresholds\":{" + "\"state\":\"Normal\"," + "\"running_slots\":2815,"
                + "\"pending_slots\":2111.89}," + "\"thresholdsGQ2LQ\":2111.0},"
                + "\"agent_url\":\"vocms0258.cern.ch\"," + "\"type\":\"site_info\"}" + "}" + "}";

        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        assertEquals(1509520209883l, metric.getTimestamp().toEpochMilli());
        assertEquals(2815f, metric.getValue().getAsFloat().get(), 0f);
        assertEquals(5, metric.getAttributes().size());
        assertTrue("test", metric.getAttributes().get("$schema").startsWith("test"));
        assertEquals("data.payload.WMBS_INFO.thresholds.running_slots", metric.getAttributes().get("$value"));
        assertEquals("T2_UK_London_Brunel", metric.getAttributes().get("data.payload.site_name"));
        assertEquals("vocms0258.cern.ch", metric.getAttributes().get("data.payload.agent_url"));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void shouldGenerateExceptionMetricWithMissingTimestamp() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
        props.setProperty(ATTRIBUTES_PARAM, "data.payload.site_name data.payload.agent_url");

        props.setProperty("value.keys.data.payload.WMBS_INFO.thresholds.running_slots",
                "data.payload.WMBS_INFO.thresholds.running_slots");
        props.setProperty("value.keys.data.payload.WMBS_INFO.thresholds.pending_slots",
                "data.payload.WMBS_INFO.thresholds.pending_slots");
        parser.config(props);

        String jsonString = "{\"metadata\":{"
                // + "\"timestamp\":1509520209883"
                + "},\"data\":{" + "\"payload\":{" + "\"site_name\":\"T2_UK_London_Brunel\","
                + "\"timestamp\":1509519908," + "\"LocalWQ_INFO\":{}," + "\"WMBS_INFO\":{" + "\"thresholds\":{"
                + "\"state\":\"Normal\"," + "\"running_slots\":2815," + "\"pending_slots\":2111.89},"
                + "\"thresholdsGQ2LQ\":2111.0}," + "\"agent_url\":\"vocms0258.cern.ch\"," + "\"type\":\"site_info\"}"
                + "}" + "}";

        JSON jsonObject = new JSON(jsonString);

        Iterator<Metric> metrics = parser.call(jsonObject.toString()).iterator();

        assertTrue(metrics.hasNext());
        Metric metric = metrics.next();
        assertNotNull(metric.getTimestamp());
        assertTrue(metric.getValue().getAsException().isPresent());
        assertEquals(5, metric.getAttributes().size());
        assertTrue("test", metric.getAttributes().get("$schema").startsWith("test"));
        assertEquals("data.payload.WMBS_INFO.thresholds.running_slots", metric.getAttributes().get("$value"));
        assertEquals("T2_UK_London_Brunel", metric.getAttributes().get("data.payload.site_name"));
        assertEquals("vocms0258.cern.ch", metric.getAttributes().get("data.payload.agent_url"));

        assertTrue(metrics.hasNext());
        metric = metrics.next();
        assertNotNull(metric.getTimestamp());
        assertTrue(metric.getValue().getAsException().isPresent());
        assertEquals(5, metric.getAttributes().size());
        assertTrue("test", metric.getAttributes().get("$schema").startsWith("test"));
        assertEquals("data.payload.WMBS_INFO.thresholds.pending_slots", metric.getAttributes().get("$value"));
        assertEquals("T2_UK_London_Brunel", metric.getAttributes().get("data.payload.site_name"));
        assertEquals("vocms0258.cern.ch", metric.getAttributes().get("data.payload.agent_url"));

        assertFalse(metrics.hasNext());
    }

    @Test
    public void shouldThrowAnExcpetionIfTimestampFormatWrongConfigured() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty("value.keys", "value");

        props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
        parser.config(props);

        props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-s");
        parser.config(props);

        props.setProperty(TIMESTAMP_FORMAT_PARAM, "YYYY-MM-DD");
        parser.config(props);

        props.setProperty(TIMESTAMP_FORMAT_PARAM, "wrong_format");
        List<Metric> result = parser.config(props).call(null);
        String resultException = result.get(0).getValue().getAsException().get();
        assertEquals(
                TIMESTAMP_FORMAT_PARAM
                        + " must be epoch-ms, epoch-s or a pattern compatible with DateTimeFormatterBuilder.",
                resultException);
    }

    @Test
    public void shouldThrowAnExcpetionIfValueNotConfigured() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        // props.setProperty("value.keys", "value");

        List<Metric> result = parser.config(props).call(null);
        assertEquals("value must be configured.", result.get(0).getValue().getAsException().get());
    }

}
