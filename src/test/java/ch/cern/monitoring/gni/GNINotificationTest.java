package ch.cern.monitoring.gni;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.httpclient.HttpException;
import org.junit.Test;

import com.google.gson.JsonParser;

import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.exdemon.monitor.trigger.action.ActionTest;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class GNINotificationTest {
	
	JsonParser JSON = new JsonParser();

	@Test
	public void shouldConvertIntegers() throws ConfigurationException, HttpException, IOException, ParseException {
        Properties properties = new Properties();
		properties.setProperty("body.metadata.metric_id", "12");
		properties.setProperty("body.metadata.snow_assignment_level", "13");
        
		GNINotification gniNotification = GNINotification.from(properties, ActionTest.DUMMY);
		
		assertEquals(12, gniNotification.getBody().get("metadata").get("metric_id"));
		assertEquals(13, gniNotification.getBody().get("metadata").get("snow_assignment_level"));
	}

	@Test
	public void shouldExtractValueFromTags() throws ConfigurationException, HttpException, IOException, ParseException {
        Properties properties = new Properties();
		properties.setProperty("header.h1", "%header_tag");
		properties.setProperty("body.metadata.metric_id", "%metric_id_tag");
		properties.setProperty("body.payload.bp1", "%payload_tag");
		properties.setProperty("body.payload.bp2", "%no-tag");
        
		Action action = ActionTest.DUMMY;
		Set<String> sinks = new HashSet<>();
		sinks.add("ALL");
		action.setActuatorIDs(sinks);
		Map<String, String> tags = new HashMap<>();
		tags.put("header_tag", "fromtag1");
		tags.put("metric_id_tag", "1234");
		tags.put("payload_tag", "fromtag2");
		action.setTags(tags);
        
		GNINotification gniNotification = GNINotification.from(properties, action);
		
		assertEquals("fromtag1", gniNotification.getHeader().get("h1"));
		assertEquals("2",  gniNotification.getHeader().get("m_version"));
		assertEquals("notification", gniNotification.getHeader().get("m_type"));
		assertTrue(gniNotification.getBody().get("metadata").containsKey("uuid"));
		assertTrue(gniNotification.getBody().get("metadata").containsKey("timestamp"));
		assertEquals("fromtag2", gniNotification.getBody().get("payload").get("bp1"));
		assertEquals("%no-tag", gniNotification.getBody().get("payload").get("bp2"));
		
		assertEquals(1234, gniNotification.getBody().get("metadata").get("metric_id"));
	}

}
