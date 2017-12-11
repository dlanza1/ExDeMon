package ch.cern.spark.metrics.schema;

import static ch.cern.spark.metrics.schema.MetricSchema.ATTRIBUTES_PARAM;
import static ch.cern.spark.metrics.schema.MetricSchema.SOURCES_PARAM;
import static ch.cern.spark.metrics.schema.MetricSchema.TIMESTAMP_ATTRIBUTE_PARAM;
import static ch.cern.spark.metrics.schema.MetricSchema.TIMESTAMP_FORMAT_PARAM;
import static ch.cern.spark.metrics.schema.MetricSchema.VALUE_ATTRIBUTES_PARAM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.text.ParseException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.json.JSONObject;
import ch.cern.spark.metrics.Metric;

public class MetricSchemaTest {

	@Test
	public void shouldParseNumbersAsValue() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.running_slots data.payload.WMBS_INFO.thresholds.pending_slots");
		props.setProperty(VALUE_ATTRIBUTES_PARAM + ".thresholdsGQ2LQ", "data.payload.WMBS_INFO.thresholdsGQ2LQ");
		
		props.setProperty(ATTRIBUTES_PARAM, "data.payload.site_name data.payload.agent_url");
		props.setProperty(ATTRIBUTES_PARAM + ".type.meta", "metadata.type");
		props.setProperty(ATTRIBUTES_PARAM + ".version", "metadata.version");
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		
		String jsonString = "{\"metadata\":{"
								+ "\"timestamp_format\":\"yyyy-MM-dd\","
								+ "\"hostname\":\"HOST1.cern.ch\","
								+ "\"type_prefix\":\"raw\","
								+ "\"producer\":\"wmagent\","
								+ "\"metric_id\":\"1234\","
								+ "\"json\":\"true\","
								+ "\"plain_text\":\"false\","
								+ "\"type\":\"metric\","
								+ "\"version\":\"001\","
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
									+ "\"type\":\"site_info\"},"
								+ "\"metadata\":{"
									+ "\"timestamp\":1509520208,"
									+ "\"id\":null}}}";
		
		JSONObject jsonObject = new JSONObject(jsonString);
		
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertEquals(2815f, metric.getValue().getAsFloat().get(), 0f);
		assertEquals(6, metric.getIDs().size());
		assertEquals("test", metric.getIDs().get("$schema"));
		assertEquals("data.payload.WMBS_INFO.thresholds.running_slots", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));
		assertEquals("metric", metric.getIDs().get("type.meta"));
		assertEquals("001", metric.getIDs().get("version"));
		
		metrics.hasNext();
		metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertEquals(2111.89f, metric.getValue().getAsFloat().get(), 0f);
		assertEquals(6, metric.getIDs().size());
		assertEquals("data.payload.WMBS_INFO.thresholds.pending_slots", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));
		assertEquals("metric", metric.getIDs().get("type.meta"));
		assertEquals("001", metric.getIDs().get("version"));
		
		metrics.hasNext();
		metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertEquals(2111.0f, metric.getValue().getAsFloat().get(), 0f);
		assertEquals(6, metric.getIDs().size());
		assertEquals("test", metric.getIDs().get("$schema"));
		assertEquals("thresholdsGQ2LQ", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));
		assertEquals("metric", metric.getIDs().get("type.meta"));
		assertEquals("001", metric.getIDs().get("version"));
		
		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldParseBooleansAsValue() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		
		props.setProperty(VALUE_ATTRIBUTES_PARAM + ".json", "metadata.json");
		props.setProperty(VALUE_ATTRIBUTES_PARAM + ".plain_text", "metadata.plain_text");
		
		props.setProperty(ATTRIBUTES_PARAM, "data.payload.site_name data.payload.agent_url");
		props.setProperty(ATTRIBUTES_PARAM + ".type.meta", "metadata.type");
		props.setProperty(ATTRIBUTES_PARAM + ".version", "metadata.version");
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		
		String jsonString = "{\"metadata\":{"
								+ "\"timestamp_format\":\"yyyy-MM-dd\","
								+ "\"hostname\":\"HOST1.cern.ch\","
								+ "\"type_prefix\":\"raw\","
								+ "\"producer\":\"wmagent\","
								+ "\"metric_id\":\"1234\","
								+ "\"json\": true,"
								+ "\"plain_text\": false,"
								+ "\"type\":\"metric\","
								+ "\"version\":\"001\","
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
									+ "\"type\":\"site_info\"},"
								+ "\"metadata\":{"
									+ "\"timestamp\":1509520208,"
									+ "\"id\":null}}}";
		
		JSONObject jsonObject = new JSONObject(jsonString);
		
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertEquals(false, metric.getValue().getAsBoolean().get());
		assertEquals(6, metric.getIDs().size());
		assertEquals("test", metric.getIDs().get("$schema"));
		assertEquals("plain_text", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));
		assertEquals("metric", metric.getIDs().get("type.meta"));
		assertEquals("001", metric.getIDs().get("version"));
		
		metrics.hasNext();
		metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertEquals(true, metric.getValue().getAsBoolean().get());
		assertEquals(6, metric.getIDs().size());
		assertEquals("test", metric.getIDs().get("$schema"));
		assertEquals("json", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));
		assertEquals("metric", metric.getIDs().get("type.meta"));
		assertEquals("001", metric.getIDs().get("version"));
		
		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldParseStringsAsValue() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		
		props.setProperty(VALUE_ATTRIBUTES_PARAM + ".payload_type", "data.payload.type");
		
		props.setProperty(ATTRIBUTES_PARAM, "data.payload.site_name data.payload.agent_url");
		props.setProperty(ATTRIBUTES_PARAM + ".type.meta", "metadata.type");
		props.setProperty(ATTRIBUTES_PARAM + ".version", "metadata.version");
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		
		String jsonString = "{\"metadata\":{"
								+ "\"timestamp_format\":\"yyyy-MM-dd\","
								+ "\"hostname\":\"HOST1.cern.ch\","
								+ "\"type_prefix\":\"raw\","
								+ "\"producer\":\"wmagent\","
								+ "\"metric_id\":\"1234\","
								+ "\"json\":\"true\","
								+ "\"plain_text\":\"false\","
								+ "\"type\":\"metric\","
								+ "\"version\":\"001\","
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
									+ "\"type\":\"site_info\"},"
								+ "\"metadata\":{"
									+ "\"timestamp\":1509520208,"
									+ "\"id\":null}}}";
		
		JSONObject jsonObject = new JSONObject(jsonString);
		
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertEquals("site_info", metric.getValue().getAsString().get());
		assertEquals(6, metric.getIDs().size());
		assertEquals("test", metric.getIDs().get("$schema"));
		assertEquals("payload_type", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));
		assertEquals("metric", metric.getIDs().get("type.meta"));
		assertEquals("001", metric.getIDs().get("version"));
		
		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldGetExceptionValueIfMissing() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		
		props.setProperty(VALUE_ATTRIBUTES_PARAM + ".payload_type", "data.payload.type");
		
		props.setProperty(ATTRIBUTES_PARAM, "data.payload.site_name data.payload.agent_url");
		props.setProperty(ATTRIBUTES_PARAM + ".type.meta", "metadata.type");
		props.setProperty(ATTRIBUTES_PARAM + ".version", "metadata.version");
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		
		String jsonString = "{\"metadata\":{"
				+ "\"timestamp_format\":\"yyyy-MM-dd\","
				+ "\"hostname\":\"HOST1.cern.ch\","
				+ "\"type_prefix\":\"raw\","
				+ "\"producer\":\"wmagent\","
				+ "\"metric_id\":\"1234\","
				+ "\"json\":\"true\","
				+ "\"plain_text\":\"false\","
				+ "\"type\":\"metric\","
				+ "\"version\":\"001\","
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
					+ "\"typeNOT\":\"site_info\"},"
				+ "\"metadata\":{"
					+ "\"timestamp\":1509520208,"
					+ "\"id\":null}}}";
		
		JSONObject jsonObject = new JSONObject(jsonString);
		
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertTrue(metric.getValue().getAsException().isPresent());
		assertEquals("No metric was generated for value key \"data.payload.type\" (alias: payload_type): document does not contian such key or is null.", 
				metric.getValue().getAsException().get());
		assertEquals(6, metric.getIDs().size());
		assertEquals("test", metric.getIDs().get("$schema"));
		assertEquals("payload_type", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));
		assertEquals("metric", metric.getIDs().get("type.meta"));
		assertEquals("001", metric.getIDs().get("version"));
		
		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void attributeNulIfNoJsonPrimitive() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.pending_slots");
		
		String att_name_no_primitive = "data.payload.WMBS_INFO.thresholds";
		props.setProperty(ATTRIBUTES_PARAM, att_name_no_primitive);
		
		String jsonString = "{\"metadata\":{"
								+ "\"timestamp\":1509520209883"
							+ "},\"data\":{"
								+ "\"payload\":{"
									+ "\"WMBS_INFO\":{"
										+ "\"thresholds\":{"
											+ "\"pending_slots\":2111.89},"
										+ "\"thresholdsGQ2LQ\":2111.0}"
								+ "}}}";
		JSONObject jsonObject = new JSONObject(jsonString);
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertNull(metric.getIDs().get(att_name_no_primitive));

		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void attributeStringFromNumber() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.pending_slots");
		
		String att_name_no_primitive = "data.payload.WMBS_INFO.thresholdsGQ2LQ";
		props.setProperty(ATTRIBUTES_PARAM, att_name_no_primitive);
		
		String jsonString = "{\"metadata\":{"
								+ "\"timestamp\":1509520209883"
							+ "},\"data\":{"
								+ "\"payload\":{"
									+ "\"WMBS_INFO\":{"
										+ "\"thresholds\":{"
											+ "\"pending_slots\":2111.89},"
										+ "\"thresholdsGQ2LQ\":2111.0}"
								+ "}}}";
		JSONObject jsonObject = new JSONObject(jsonString);
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals("2111.0", metric.getIDs().get(att_name_no_primitive));

		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldParseTimestampWithFormatInMs() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.pending_slots");
		
		String jsonString = "{\"metadata\":{"
								+ "\"timestamp\":1509520209883"
							+ "},\"data\":{"
								+ "\"payload\":{"
									+ "\"WMBS_INFO\":{"
										+ "\"thresholds\":{"
											+ "\"pending_slots\":2111.89},"
										+ "\"thresholdsGQ2LQ\":2111.0}"
								+ "}}}";
		JSONObject jsonObject = new JSONObject(jsonString);
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());

		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldParseTimestampWithFormatInS() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-s");
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.pending_slots");
		
		String jsonString = "{\"metadata\":{"
								+ "\"timestamp\":1509520209"
							+ "},\"data\":{"
								+ "\"payload\":{"
									+ "\"WMBS_INFO\":{"
										+ "\"thresholds\":{"
											+ "\"pending_slots\":2111.89},"
										+ "\"thresholdsGQ2LQ\":2111.0}"
								+ "}}}";
		JSONObject jsonObject = new JSONObject(jsonString);
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals(1509520209000l, metric.getInstant().toEpochMilli());
		
		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldParseDateTimeTimestampWithFormatInDateFormat() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "yyyy-MM-dd HH:mm:ssZ");
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.pending_slots");
		
		String jsonString = "{\"metadata\":{"
								+ "\"timestamp\":\"2017-10-20 02:00:12+0000\""
							+ "},\"data\":{"
								+ "\"payload\":{"
									+ "\"WMBS_INFO\":{"
										+ "\"thresholds\":{"
											+ "\"pending_slots\":2111.89},"
										+ "\"thresholdsGQ2LQ\":2111.0}"
								+ "}}}";
		JSONObject jsonObject = new JSONObject(jsonString);
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();
		
		metrics.hasNext();
		Metric metric = metrics.next();
		// Value should be --> Fri Oct 20 2017 00:00:12 UTC
	    Instant timestamp = Instant.parse("2017-10-20T02:00:12.000Z");
		assertEquals(timestamp.toEpochMilli(), metric.getInstant().toEpochMilli());
		
		assertFalse(metrics.hasNext());
	}
	
    @Test
    public void shouldParseDateTimeTimestampWithFormatInDateFormatWithoutTimeZone() throws ParseException, ConfigurationException {
        Properties props = new Properties();
        props.setProperty(SOURCES_PARAM, "test");
        props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
        props.setProperty(TIMESTAMP_FORMAT_PARAM, "yyyy-MM-dd HH:mm:ss");
        props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.pending_slots");

        String jsonString = "{\"metadata\":{"
                                + "\"timestamp\":\"2017-12-20 02:00:12\""
                          + "},\"data\":{"
                              + "\"payload\":{"
                                  + "\"WMBS_INFO\":{"
                                      + "\"thresholds\":{"
                                          + "\"pending_slots\":2111.89},"
                                      + "\"thresholdsGQ2LQ\":2111.0}"
                          + "}}}";
        JSONObject jsonObject = new JSONObject(jsonString);

        MetricSchema parser = new MetricSchema("test");
        parser.config(props);
        Iterator<Metric> metrics = parser.call(jsonObject).iterator();

        metrics.hasNext();
        Metric metric = metrics.next();
        
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy-MM-dd HH:mm:ss").toFormatter().withZone(ZoneId.systemDefault());
        
        // Value should be --> Fri Oct 20 2017 00:00:12 UTC
        Instant timestamp = Instant.from(formatter.parse("2017-12-20 02:00:12"));
        assertEquals(timestamp, metric.getInstant());

        assertFalse(metrics.hasNext());
    }
	
	@Test
	public void shouldParseDateTimestampWithFormatInDateFormat() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "yyyy MM dd");
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.pending_slots");
		
		String jsonString = "{\"metadata\":{"
								+ "\"timestamp\":\"2017 12 20\""
							+ "},\"data\":{"
								+ "\"payload\":{"
									+ "\"WMBS_INFO\":{"
										+ "\"thresholds\":{"
											+ "\"pending_slots\":2111.89},"
										+ "\"thresholdsGQ2LQ\":2111.0}"
								+ "}}}";
		JSONObject jsonObject = new JSONObject(jsonString);
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();
		
		metrics.hasNext();
		Metric metric = metrics.next();
		
		DateTimeFormatter formatter = new DateTimeFormatterBuilder().appendPattern("yyyy MM dd HH:mm:ss").toFormatter().withZone(ZoneId.systemDefault());
		
		Instant timestamp = Instant.from(formatter.parse("2017 12 20 00:00:00"));
		assertEquals(timestamp, metric.getInstant());
		
		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldGenerateExceptionMetricIfTimestampHasWrongFormatInDateFormat() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "yyyy MM dd");
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.pending_slots");
		
		String jsonString = "{\"metadata\":{"
								+ "\"timestamp\":\"10:20:12\""
							+ "},\"data\":{"
								+ "\"payload\":{"
									+ "\"WMBS_INFO\":{"
										+ "\"thresholds\":{"
											+ "\"pending_slots\":2111.89},"
										+ "\"thresholdsGQ2LQ\":2111.0}"
								+ "}}}";
		JSONObject jsonObject = new JSONObject(jsonString);
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();

		assertTrue(metrics.hasNext());
		Metric metric = metrics.next();
		assertTrue(metric.getValue().getAsException().isPresent());
		
		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldGenerateExceptionMetricIfTimestampHasWrongFormatInEpoch() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.pending_slots");
		
		String jsonString = "{\"metadata\":{"
								+ "\"timestamp\":\"10:20:12\""
							+ "},\"data\":{"
								+ "\"payload\":{"
									+ "\"WMBS_INFO\":{"
										+ "\"thresholds\":{"
											+ "\"pending_slots\":2111.89},"
										+ "\"thresholdsGQ2LQ\":2111.0}"
								+ "}}}";
		JSONObject jsonObject = new JSONObject(jsonString);
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();

		assertTrue(metrics.hasNext());
		Metric metric = metrics.next();
		assertNotNull(metric.getInstant());
		assertTrue(metric.getValue().getAsException().isPresent());
		assertEquals(2, metric.getIDs().size());
		assertEquals("test", metric.getIDs().get("$schema"));
		assertEquals("data.payload.WMBS_INFO.thresholds.pending_slots", metric.getIDs().get("$value_attribute"));

		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldParseWithMissingAttributes() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.running_slots");
		
		props.setProperty(ATTRIBUTES_PARAM, "data.payload.site_name data.payload.agent_url");
		props.setProperty(ATTRIBUTES_PARAM + ".type.meta", "metadata.type");
		props.setProperty(ATTRIBUTES_PARAM + ".version", "metadata.version");
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		
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
		
		JSONObject jsonObject = new JSONObject(jsonString);
		
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertEquals(2815f, metric.getValue().getAsFloat().get(), 0f);
		assertEquals(4, metric.getIDs().size());
		assertEquals("test", metric.getIDs().get("$schema"));
		assertEquals("data.payload.WMBS_INFO.thresholds.running_slots", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));

		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldGenerateExceptionMetricWhenMissingValue() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		props.setProperty(ATTRIBUTES_PARAM, "data.payload.site_name data.payload.agent_url");
		
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.running_slots data.payload.WMBS_INFO.thresholds.pending_slots");
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		
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
//											+ "\"running_slots\":2815,"
											+ "\"pending_slots\":2111.89},"
										+ "\"thresholdsGQ2LQ\":2111.0},"
									+ "\"agent_url\":\"vocms0258.cern.ch\","
									+ "\"type\":\"site_info\"}"
								+ "}"
							+ "}";
		
		JSONObject jsonObject = new JSONObject(jsonString);
		
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();
		
		assertTrue(metrics.hasNext());
		Metric metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertTrue(metric.getValue().getAsException().isPresent());
		assertEquals(4, metric.getIDs().size());
		assertEquals("test", metric.getIDs().get("$schema"));
		assertEquals("data.payload.WMBS_INFO.thresholds.running_slots", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));
		
		assertTrue(metrics.hasNext());
		metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertEquals(2111.89f, metric.getValue().getAsFloat().get(), 0f);
		assertEquals(4, metric.getIDs().size());
		assertEquals("test", metric.getIDs().get("$schema"));
		assertEquals("data.payload.WMBS_INFO.thresholds.pending_slots", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));

		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldGenerateExceptionMetricWithMissingTimestamp() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		props.setProperty(ATTRIBUTES_PARAM, "data.payload.site_name data.payload.agent_url");
		
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.running_slots data.payload.WMBS_INFO.thresholds.pending_slots");
		
		MetricSchema parser = new MetricSchema("test");
		parser.config(props);
		
		String jsonString = "{\"metadata\":{"
//								+ "\"timestamp\":1509520209883"
							+ "},\"data\":{"
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
		
		JSONObject jsonObject = new JSONObject(jsonString);
		
		Iterator<Metric> metrics = parser.call(jsonObject).iterator();

		assertTrue(metrics.hasNext());
		Metric metric = metrics.next();
		assertNotNull(metric.getInstant());
		assertTrue(metric.getValue().getAsException().isPresent());
		assertEquals(4, metric.getIDs().size());
		assertEquals("test", metric.getIDs().get("$schema"));
		assertEquals("data.payload.WMBS_INFO.thresholds.running_slots", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));
		
		assertTrue(metrics.hasNext());
		metric = metrics.next();
		assertNotNull(metric.getInstant());
		assertTrue(metric.getValue().getAsException().isPresent());
		assertEquals(4, metric.getIDs().size());
		assertEquals("test", metric.getIDs().get("$schema"));
		assertEquals("data.payload.WMBS_INFO.thresholds.pending_slots", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));

		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldThrowAnExcpetionIfTimestampNotConfigured() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
//		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "value");

		
		List<Metric> result = new MetricSchema("test").config(props).call(null);
		assertEquals(TIMESTAMP_ATTRIBUTE_PARAM + " must be configured.", result.get(0).getValue().getAsException().get());
	}
	
	@Test
	public void shouldThrowAnExcpetionIfTimestampFormatWrongConfigured() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "value");

		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		new MetricSchema("test").config(props);
		
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-s");
		new MetricSchema("test").config(props);
		
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "YYYY-MM-DD");
		new MetricSchema("test").config(props);
		
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "wrong_format");
		List<Metric> result = new MetricSchema("test").config(props).call(null);
		String resultException = result.get(0).getValue().getAsException().get();
		assertEquals(TIMESTAMP_FORMAT_PARAM + " must be epoch-ms, epoch-s or a pattern compatible with DateTimeFormatterBuilder.", resultException);
	}
	
	@Test
	public void shouldThrowAnExcpetionIfValueNotConfigured() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(SOURCES_PARAM, "test");
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
//		props.setProperty(VALUE_ATTRIBUTES_PARAM, "value");
		
		List<Metric> result = new MetricSchema("test").config(props).call(null);
		assertEquals(VALUE_ATTRIBUTES_PARAM + " must be configured.", result.get(0).getValue().getAsException().get());
	}
	
}
