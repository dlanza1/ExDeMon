package ch.cern.spark.http;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.time.Instant;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.Stream;
import ch.cern.spark.StreamTestHelper;
import ch.cern.spark.http.HTTPSink;
import ch.cern.spark.metrics.results.AnalysisResult;
import java.text.SimpleDateFormat;
import java.util.Date;

public class HTTPSinkTest extends StreamTestHelper<AnalysisResult, AnalysisResult>{
	
	private static final long serialVersionUID = -8846451662432392890L;

	@Test
	public void send() throws ConfigurationException, HttpException, IOException {
		HttpClient httpClient = mock(HttpClient.class, withSettings().serializable());
		when(httpClient.executeMethod(anyObject())).thenReturn(201);
		
		HTTPSink.setHTTPClient(httpClient);
		
        Properties properties = new Properties();
		properties.setProperty("url", "http://localhost:1234");
		properties.setProperty("add.key1", "key1");
		properties.setProperty("add.key2.a1", "key2");
        
                Instant instant = Instant.ofEpochMilli(0);
		AnalysisResult analysisResult = new AnalysisResult();
		analysisResult.setAnalysisTimestamp(instant);
		addInput(0, analysisResult);
        
        Stream<AnalysisResult> resultsStream = createStream(AnalysisResult.class);
        
        HTTPSink sink = new HTTPSink();
        sink.config(properties);
        sink.sink(resultsStream);
		
		start();
		
		ArgumentCaptor<PostMethod> methodCaptor = ArgumentCaptor.forClass(PostMethod.class);
		verify(httpClient, times(1)).executeMethod(methodCaptor.capture());
                
		StringRequestEntity receivedEntity = (StringRequestEntity) methodCaptor.getAllValues().get(0).getRequestEntity();
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
                String expectedTimestamp = sdf.format(Date.from(instant));
		assertEquals("[{\"analysis_timestamp\":\"" + expectedTimestamp + "\","
					+ "\"analysis_params\":{},"
					+ "\"tags\":{},"
					+ "\"key1\":\"key1\","
					+ "\"key2\":{\"a1\":\"key2\"}}]", receivedEntity.getContent());
	}

}
