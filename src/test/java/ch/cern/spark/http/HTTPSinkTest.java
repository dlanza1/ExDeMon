package ch.cern.spark.http;

import static org.junit.Assert.assertEquals;
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

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.json.JSONObject;
import ch.cern.spark.json.JSONParser;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.results.AnalysisResult;

public class HTTPSinkTest {

	@Test
	public void send() throws ConfigurationException, HttpException, IOException {
		HttpClient httpClient = mock(HttpClient.class, withSettings().serializable());
		when(httpClient.executeMethod(anyObject())).thenReturn(201);
		
		HTTPSink.setHTTPClient(httpClient);
        
        Instant instant = Instant.now();
		AnalysisResult analysisResult = new AnalysisResult();
		analysisResult.setAnalysisTimestamp(instant);
        
        Properties properties = new Properties();
        properties.setProperty("url", "http://localhost:1234");
        HTTPSink sink = new HTTPSink();
        sink.config(properties);
        
        sink.send(Arrays.asList(JSONParser.parse(analysisResult).toString()).iterator());
		
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
        properties.setProperty("add.header.h1", "%header_tag");
        properties.setProperty("add.body.metadata.metric_id", "%metric_id_tag");
        properties.setProperty("add.body.payload.bp1", "%payload_tag");
        properties.setProperty("add.body.payload.bp2", "%no-tag");
        HTTPSink sink = new HTTPSink();
        sink.config(properties);
        
        Notification notification = new Notification();
        Set<String> sinks = new HashSet<>();
        sinks.add("ALL");
        notification.setSink_ids(sinks);
        Map<String, String> tags = new HashMap<>();
        tags.put("header_tag", "fromtag1");
        tags.put("metric_id_tag", "1234");
        tags.put("payload_tag", "fromtag2");
        notification.setTags(tags);
        
        JSONObject jsonResult = sink.processProperties(notification);
        
        assertEquals("{\"tags\":{\"metric_id_tag\":\"1234\",\"payload_tag\":\"fromtag2\",\"header_tag\":\"fromtag1\"},"
                    + "\"sink_ids\":[\"ALL\"],"
                    + "\"header\":{\"h1\":\"fromtag1\"},"
                    + "\"body\":{"
                        + "\"payload\":{\"bp1\":\"fromtag2\",\"bp2\":null},"
                        + "\"metadata\":{\"metric_id\":\"1234\"}}}", jsonResult.toString());
    }

}
