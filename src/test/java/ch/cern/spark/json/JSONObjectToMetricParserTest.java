package ch.cern.spark.json;

import static ch.cern.spark.json.JSONObjectToMetricParser.ATTRIBUTES_PARAM;
import static ch.cern.spark.json.JSONObjectToMetricParser.TIMESTAMP_ATTRIBUTE_PARAM;
import static ch.cern.spark.json.JSONObjectToMetricParser.TIMESTAMP_FORMAT_PARAM;
import static ch.cern.spark.json.JSONObjectToMetricParser.VALUE_ATTRIBUTES_PARAM;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.text.ParseException;
import java.util.Iterator;

import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.Metric;

public class JSONObjectToMetricParserTest {

	@Test
	public void shouldParse() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.running_slots data.payload.WMBS_INFO.thresholds.pending_slots");
		props.setProperty(VALUE_ATTRIBUTES_PARAM + ".thresholdsGQ2LQ", "data.payload.WMBS_INFO.thresholdsGQ2LQ");
		
		props.setProperty(ATTRIBUTES_PARAM, "data.payload.site_name data.payload.agent_url");
		props.setProperty(ATTRIBUTES_PARAM + ".type.meta", "metadata.type");
		props.setProperty(ATTRIBUTES_PARAM + ".version", "metadata.version");
		
		JSONObjectToMetricParser parser = new JSONObjectToMetricParser(props);
		
		String jsonString = "{\"metadata\":{"
								+ "\"timestamp_format\":\"yyyy-MM-dd\","
								+ "\"hostname\":\"HOST1.cern.ch\","
								+ "\"type_prefix\":\"raw\","
								+ "\"producer\":\"wmagent\","
								+ "\"metric_id\":\"1234\","
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
		
		Iterator<Metric> metrics = parser.call(jsonObject);
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertEquals(2815f, metric.getValue(), 0f);
		assertEquals(5, metric.getIDs().size());
		assertEquals("data.payload.WMBS_INFO.thresholds.running_slots", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));
		assertEquals("metric", metric.getIDs().get("type.meta"));
		assertEquals("001", metric.getIDs().get("version"));
		
		metrics.hasNext();
		metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertEquals(2111.89f, metric.getValue(), 0f);
		assertEquals(5, metric.getIDs().size());
		assertEquals("data.payload.WMBS_INFO.thresholds.pending_slots", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));
		assertEquals("metric", metric.getIDs().get("type.meta"));
		assertEquals("001", metric.getIDs().get("version"));
		
		metrics.hasNext();
		metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertEquals(2111.0f, metric.getValue(), 0f);
		assertEquals(5, metric.getIDs().size());
		assertEquals("thresholdsGQ2LQ", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));
		assertEquals("metric", metric.getIDs().get("type.meta"));
		assertEquals("001", metric.getIDs().get("version"));
		
		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldParseTimestampWithFormatInMs() throws ParseException, ConfigurationException {
		Properties props = new Properties();
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
		
		JSONObjectToMetricParser parser = new JSONObjectToMetricParser(props);
		Iterator<Metric> metrics = parser.call(jsonObject);
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());

		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldParseTimestampWithFormatInS() throws ParseException, ConfigurationException {
		Properties props = new Properties();
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
		
		JSONObjectToMetricParser parser = new JSONObjectToMetricParser(props);
		Iterator<Metric> metrics = parser.call(jsonObject);
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals(1509520209000l, metric.getInstant().toEpochMilli());
		
		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldParseDateTimeTimestampWithFormatInDateFormat() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "yyyy-MM-dd HH:mm:ss");
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.pending_slots");
		
		String jsonString = "{\"metadata\":{"
								+ "\"timestamp\":\"2017-10-20 02:00:12\""
							+ "},\"data\":{"
								+ "\"payload\":{"
									+ "\"WMBS_INFO\":{"
										+ "\"thresholds\":{"
											+ "\"pending_slots\":2111.89},"
										+ "\"thresholdsGQ2LQ\":2111.0}"
								+ "}}}";
		JSONObject jsonObject = new JSONObject(jsonString);
		
		JSONObjectToMetricParser parser = new JSONObjectToMetricParser(props);
		Iterator<Metric> metrics = parser.call(jsonObject);
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals(1508457612000l, metric.getInstant().toEpochMilli());
		
		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldParseDateTimestampWithFormatInDateFormat() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "yyyy MM dd");
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.pending_slots");
		
		String jsonString = "{\"metadata\":{"
								+ "\"timestamp\":\"2017 10 20\""
							+ "},\"data\":{"
								+ "\"payload\":{"
									+ "\"WMBS_INFO\":{"
										+ "\"thresholds\":{"
											+ "\"pending_slots\":2111.89},"
										+ "\"thresholdsGQ2LQ\":2111.0}"
								+ "}}}";
		JSONObject jsonObject = new JSONObject(jsonString);
		
		JSONObjectToMetricParser parser = new JSONObjectToMetricParser(props);
		Iterator<Metric> metrics = parser.call(jsonObject);
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals("2017-10-19T22:00:00Z", metric.getInstant().toString());
		
		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldNotParseIfTimestampHasWrongFormatInDateFormat() throws ParseException, ConfigurationException {
		Properties props = new Properties();
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
		
		JSONObjectToMetricParser parser = new JSONObjectToMetricParser(props);
		Iterator<Metric> metrics = parser.call(jsonObject);

		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldNotParseIfTimestampHasWrongFormatInEpoch() throws ParseException, ConfigurationException {
		Properties props = new Properties();
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
		
		JSONObjectToMetricParser parser = new JSONObjectToMetricParser(props);
		Iterator<Metric> metrics = parser.call(jsonObject);

		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldParseWithMissingAttributes() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.running_slots");
		
		props.setProperty(ATTRIBUTES_PARAM, "data.payload.site_name data.payload.agent_url");
		props.setProperty(ATTRIBUTES_PARAM + ".type.meta", "metadata.type");
		props.setProperty(ATTRIBUTES_PARAM + ".version", "metadata.version");
		
		JSONObjectToMetricParser parser = new JSONObjectToMetricParser(props);
		
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
		
		Iterator<Metric> metrics = parser.call(jsonObject);
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertEquals(2815f, metric.getValue(), 0f);
		assertEquals(3, metric.getIDs().size());
		assertEquals("data.payload.WMBS_INFO.thresholds.running_slots", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));

		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldNotParseWithMissingValue() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		props.setProperty(ATTRIBUTES_PARAM, "data.payload.site_name data.payload.agent_url");
		
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.running_slots data.payload.WMBS_INFO.thresholds.pending_slots");
		
		JSONObjectToMetricParser parser = new JSONObjectToMetricParser(props);
		
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
		
		Iterator<Metric> metrics = parser.call(jsonObject);
		
		metrics.hasNext();
		Metric metric = metrics.next();
		assertEquals(1509520209883l, metric.getInstant().toEpochMilli());
		assertEquals(2111.89f, metric.getValue(), 0f);
		assertEquals(3, metric.getIDs().size());
		assertEquals("data.payload.WMBS_INFO.thresholds.pending_slots", metric.getIDs().get("$value_attribute"));
		assertEquals("T2_UK_London_Brunel", metric.getIDs().get("data.payload.site_name"));
		assertEquals("vocms0258.cern.ch", metric.getIDs().get("data.payload.agent_url"));

		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldNotParseWithMissingTimestamp() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		props.setProperty(ATTRIBUTES_PARAM, "data.payload.site_name data.payload.agent_url");
		
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "data.payload.WMBS_INFO.thresholds.running_slots data.payload.WMBS_INFO.thresholds.pending_slots");
		
		JSONObjectToMetricParser parser = new JSONObjectToMetricParser(props);
		
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
		
		Iterator<Metric> metrics = parser.call(jsonObject);

		assertFalse(metrics.hasNext());
	}
	
	@Test
	public void shouldThrowAnExcpetionIfTimestampNotConfigured() throws ParseException, ConfigurationException {
		Properties props = new Properties();
//		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "value");

		try {
			new JSONObjectToMetricParser(props);
			
			fail();
		}catch(ConfigurationException e) {
			assertEquals(TIMESTAMP_ATTRIBUTE_PARAM + " must be configured.", e.getMessage());
		}
	}
	
	@Test
	public void shouldThrowAnExcpetionIfTimestampFormatWrongConfigured() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
		props.setProperty(VALUE_ATTRIBUTES_PARAM, "value");

		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-ms");
		new JSONObjectToMetricParser(props);
		
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "epoch-s");
		new JSONObjectToMetricParser(props);
		
		props.setProperty(TIMESTAMP_FORMAT_PARAM, "YYYY-MM-DD");
		new JSONObjectToMetricParser(props);
		
		try {
			props.setProperty(TIMESTAMP_FORMAT_PARAM, "wrong_format");
			new JSONObjectToMetricParser(props);
			
			fail();
		}catch(ConfigurationException e) {
			assertEquals(TIMESTAMP_FORMAT_PARAM + " must be epoch-ms, epoch-s or a pattern compatible with DateTimeFormatterBuilder.", e.getMessage());
		}
	}
	
	@Test
	public void shouldThrowAnExcpetionIfValueNotConfigured() throws ParseException, ConfigurationException {
		Properties props = new Properties();
		props.setProperty(TIMESTAMP_ATTRIBUTE_PARAM, "metadata.timestamp");
//		props.setProperty(VALUE_ATTRIBUTES_PARAM, "value");
		
		try {
			new JSONObjectToMetricParser(props);
			
			fail();
		}catch(ConfigurationException e) {
			assertEquals(VALUE_ATTRIBUTES_PARAM + " must be configured.", e.getMessage());
		}
	}
	
}
