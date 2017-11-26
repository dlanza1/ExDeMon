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

public class HTTPSinkTest extends StreamTestHelper<AnalysisResult, AnalysisResult>{
	
	private static final long serialVersionUID = -8846451662432392890L;

	@Test
	public void send() throws ConfigurationException, HttpException, IOException {
		HttpClient httpClient = mock(HttpClient.class);
		when(httpClient.executeMethod(anyObject())).thenReturn(201);
		
		HTTPSink.setClient(httpClient);
		
        Properties properties = new Properties();
		properties.setProperty("url", "http://localhost:1234");
		properties.setProperty("add.key1", "key1");
		properties.setProperty("add.key2.a1", "key2");
        
		AnalysisResult analysisResult = new AnalysisResult();
		analysisResult.setAnalysisTimestamp(Instant.ofEpochMilli(0));
		addInput(0, analysisResult);
        
        Stream<AnalysisResult> resultsStream = createStream(AnalysisResult.class);
        
        HTTPSink sink = new HTTPSink();
        sink.config(properties);
        sink.sink(resultsStream);
		
		start();
		
		ArgumentCaptor<PostMethod> methodCaptor = ArgumentCaptor.forClass(PostMethod.class);
		verify(httpClient, times(1)).executeMethod(methodCaptor.capture());
		
		StringRequestEntity receivedEntity = (StringRequestEntity) methodCaptor.getAllValues().get(0).getRequestEntity();
		assertEquals("{\"analysis_timestamp\":\"1970-01-01T01:00:00+0100\","
					+ "\"analysis_params\":{},"
					+ "\"tags\":{},"
					+ "\"key1\":\"key1\","
					+ "\"key2\":{\"a1\":\"key2\"}}", receivedEntity.getContent());
	}

}
