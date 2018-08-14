package ch.cern.exdemon.http;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.Arrays;
import java.util.Date;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import ch.cern.exdemon.json.JSONParser;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
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

}
