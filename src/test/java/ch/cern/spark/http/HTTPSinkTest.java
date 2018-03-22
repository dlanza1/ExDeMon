package ch.cern.spark.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.gson.JsonPrimitive;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.json.JSONParser;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.trigger.action.Action;
import ch.cern.spark.metrics.trigger.action.ActionTest;

public class HTTPSinkTest {

	@Test
	public void send() throws ConfigurationException, HttpException, IOException {
		HttpClient httpClient = mock(HttpClient.class, withSettings().serializable());
		when(httpClient.executeMethod(anyObject())).thenReturn(201);
		
		HTTPSink.setHTTPClient(httpClient);
        
		Properties properties = new Properties();
        properties.setProperty("url", "http://localhost:1234");
        HTTPSink sink = new HTTPSink();
        sink.config(properties);
        
        Instant instant = Instant.now();
		AnalysisResult analysisResult = new AnalysisResult();
		analysisResult.setAnalysis_timestamp(instant);

        sink.send(Arrays.asList(new JsonPOSTRequest("", JSONParser.parse(analysisResult))).iterator());
		
		ArgumentCaptor<PostMethod> methodCaptor = ArgumentCaptor.forClass(PostMethod.class);
		verify(httpClient, times(1)).executeMethod(methodCaptor.capture());
                
		StringRequestEntity receivedEntity = (StringRequestEntity) methodCaptor.getAllValues().get(0).getRequestEntity();
		
        String expectedTimestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(Date.from(instant));
        
		assertEquals("[{\"analysis_timestamp\":\"" + expectedTimestamp + "\","
    					+ "\"analysis_params\":{},"
    					+ "\"tags\":{}}]", receivedEntity.getContent());
	}
	
    @Test
    public void shouldExtractValueFromTags() throws ConfigurationException, HttpException, IOException, ParseException {
        Properties properties = new Properties();
        properties.setProperty("url", "https://abcd.cern.ch/<tags:url-suffix>");
        properties.setProperty("add.header.h1", "<tags:header_tag>");
        properties.setProperty("add.body.metadata.metric_id", "<tags:metric_id_tag>");
        properties.setProperty("add.body.payload.bp1", "<tags:payload_tag>");
        properties.setProperty("add.body.payload.bp2", "<tags:no-tag>");
        HTTPSink sink = new HTTPSink();
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
    
    @Test
    public void shouldAddArrayOfKeys() throws ConfigurationException, HttpException, IOException, ParseException {
        Properties properties = new Properties();
        properties.setProperty("url", "https://abcd.cern.ch/<tags:url-suffix>");
        properties.setProperty("add.idb_tags", "[keys:analyzed_metric.attributes.*]");
        HTTPSink sink = new HTTPSink();
        sink.config(properties);
        
        AnalysisResult analysis = new AnalysisResult();
        
        Map<String, String> metric_attributes = new HashMap<>();
        metric_attributes.put("$value", "value1");
        metric_attributes.put("att1", "att1-value");
        metric_attributes.put("att2", "att2-value");
        Metric metric = new Metric(Instant.now(), 10f, metric_attributes);
        analysis.setAnalyzedMetric(metric );
        
        JsonPOSTRequest jsonResult = sink.toJsonPOSTRequest(analysis);
        
        assertEquals(new JsonPrimitive("analyzed_metric.attributes.$value"), jsonResult.getJson().getElement("idb_tags").getAsJsonArray().get(2));
        assertEquals(new JsonPrimitive("analyzed_metric.attributes.att1"), jsonResult.getJson().getElement("idb_tags").getAsJsonArray().get(1));
        assertEquals(new JsonPrimitive("analyzed_metric.attributes.att2"), jsonResult.getJson().getElement("idb_tags").getAsJsonArray().get(0));
    }

}
