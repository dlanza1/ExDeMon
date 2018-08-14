package ch.cern.exdemon.monitor.trigger.action.actuator.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.http.HttpException;
import org.junit.Test;

import ch.cern.exdemon.http.JsonPOSTRequest;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.exdemon.monitor.trigger.action.ActionTest;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class HTTPActuatorTest {

    @Test
    public void shouldExtractValueFromTags() throws ConfigurationException, HttpException, IOException, ParseException {
        Properties properties = new Properties();
        properties.setProperty("url", "https://abcd.cern.ch/<tags:url-suffix>");
        properties.setProperty("add.header.h1", "<tags:header_tag>");
        properties.setProperty("add.body.metadata.metric_id", "<tags:metric_id_tag>");
        properties.setProperty("add.body.payload.bp1", "<tags:payload_tag>");
        properties.setProperty("add.body.payload.bp2", "<tags:no-tag>");
        HTTPActuator sink = new HTTPActuator();
        sink.config(properties);
        
        Action action = ActionTest.DUMMY;
        Set<String> sinks = new HashSet<>();
        sinks.add("ALL");
        action.setActuatorIDs(sinks);
        Map<String, String> tags = new HashMap<>();
        tags.put("url-suffix", "/job/id/23/");
        tags.put("header_tag", "fromtag1");
        tags.put("metric_id_tag", "1234");
        tags.put("payload_tag", "fromtag2");
        action.setTags(tags);
        
        JsonPOSTRequest jsonResult = sink.toJsonPOSTRequest(action);
        
        assertEquals("https://abcd.cern.ch//job/id/23/", jsonResult.getUrl());
        
        assertEquals("/job/id/23/", jsonResult.getJson().getProperty("tags.url-suffix"));
        assertEquals("1234", jsonResult.getJson().getProperty("tags.metric_id_tag"));
        assertEquals("fromtag1", jsonResult.getJson().getProperty("tags.header_tag"));
        assertEquals("fromtag2", jsonResult.getJson().getProperty("tags.payload_tag"));
        assertEquals("fromtag1", jsonResult.getJson().getProperty("header.h1"));
        
        assertEquals("fromtag2", jsonResult.getJson().getProperty("body.payload.bp1"));
        assertNull(jsonResult.getJson().getProperty("body.payload.bp2"));
        assertEquals("1234", jsonResult.getJson().getProperty("body.metadata.metric_id"));
    }
    
}
