package ch.cern.exdemon.http;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.gson.JsonPrimitive;

import ch.cern.exdemon.json.JSONParser;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.exdemon.monitor.trigger.action.ActionTest;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class HTTPSinkTest {

	@Test
	public void send() throws ConfigurationException, HttpException, IOException {
		HttpClient httpClient = mock(HttpClient.class, withSettings().serializable());
		HttpResponse response = mock(HttpResponse.class, withSettings().serializable());
		StatusLine statusLine = mock(StatusLine.class, withSettings().serializable());
		when(statusLine.getStatusCode()).thenReturn(201);
        when(response.getStatusLine()).thenReturn(statusLine);
        when(httpClient.execute(anyObject())).thenReturn(response);
		
		HTTPSink.setHTTPClient(httpClient);
        
		Properties properties = new Properties();
        properties.setProperty("url", "http://localhost:1234");
        HTTPSink sink = new HTTPSink();
        sink.config(properties);
        
        Instant instant = Instant.now();
		AnalysisResult analysisResult = new AnalysisResult();
		analysisResult.setAnalysis_timestamp(instant);
		analysisResult.setTimestamp(instant.toEpochMilli());

        sink.batchAndSend(Arrays.asList(new JsonPOSTRequest("", JSONParser.parse(analysisResult))).iterator());
		
		ArgumentCaptor<HttpPost> methodCaptor = ArgumentCaptor.forClass(HttpPost.class);
		verify(httpClient, times(1)).execute(methodCaptor.capture());
                
		HttpPost receivedEntity = methodCaptor.getAllValues().get(0);
		
        String expectedTimestamp = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(Date.from(instant));
        
		assertEquals("[{\"timestamp\":" + instant.toEpochMilli() +","
		                + "\"analysis_timestamp\":\"" + expectedTimestamp + "\","
    					+ "\"analysis_params\":{},"
    					+ "\"tags\":{}}]", IOUtils.toString(receivedEntity.getEntity().getContent(), StandardCharsets.UTF_8));
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
    
    @Test
    public void shouldAddArrayOfKeysAndAttribues() throws ConfigurationException, HttpException, IOException, ParseException {
        Properties properties = new Properties();
        properties.setProperty("url", "https://abcd.cern.ch/<tags:url-suffix>");
        properties.setProperty("add.idb_tags", "[keys:analyzed_metric.attributes.\\Q$\\E.*++attributes:#idb_tags.att]");
        HTTPSink sink = new HTTPSink();
        sink.config(properties);
        
        AnalysisResult analysis = new AnalysisResult();
        
        Map<String, String> metric_attributes = new HashMap<>();
        metric_attributes.put("$value", "value1");
        metric_attributes.put("att1", "att1-value");
        metric_attributes.put("att2", "att2-value");
        metric_attributes.put("att3", "att3-value");
        Metric metric = new Metric(Instant.now(), 10f, metric_attributes);
        analysis.setAnalyzedMetric(metric);
        Map<String, String> tags = new HashMap<>();
        tags.put("idb_tags.att", "att1 att2 att4");
        analysis.setTags(tags);
        
        JsonPOSTRequest jsonResult = sink.toJsonPOSTRequest(analysis);

        assertEquals(3, jsonResult.getJson().getElement("idb_tags").getAsJsonArray().size());
        assertEquals(new JsonPrimitive("analyzed_metric.attributes.$value"), jsonResult.getJson().getElement("idb_tags").getAsJsonArray().get(0));
        assertEquals(new JsonPrimitive("analyzed_metric.attributes.att1"), jsonResult.getJson().getElement("idb_tags").getAsJsonArray().get(1));
        assertEquals(new JsonPrimitive("analyzed_metric.attributes.att2"), jsonResult.getJson().getElement("idb_tags").getAsJsonArray().get(2));
    }

}
