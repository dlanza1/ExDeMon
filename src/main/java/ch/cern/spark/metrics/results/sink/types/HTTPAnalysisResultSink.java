package ch.cern.spark.metrics.results.sink.types;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.Stream;
import ch.cern.spark.json.JSONObject;
import ch.cern.spark.metrics.results.AnalysisResult;
import ch.cern.spark.metrics.results.sink.AnalysisResultsSink;

@RegisterComponent("http")
public class HTTPAnalysisResultSink extends AnalysisResultsSink {
	
	private final static Logger LOG = Logger.getLogger(HTTPAnalysisResultSink.class.getName());

	private static final long serialVersionUID = 6368509840922047167L;

	public static final String AUTH_PROPERTY = "auth";
    public static final String AUTH_USERNAME_PROPERTY = AUTH_PROPERTY + ".user";
    public static final String AUTH_PASSWORD_PROPERTY = AUTH_PROPERTY + ".password";
    private Header authHeader;

	private String url;
	
	private static transient HttpClient httpClient = new HttpClient();
	
	private Map<String, String> propertiesToAdd;

	@Override
	public void config(Properties properties) throws ConfigurationException {
		super.config(properties);
		
		url = properties.getProperty("url");
		
		Properties addProperties = properties.getSubset("add");
		Set<String> keysToAdd = addProperties.getUniqueKeyFields();
		propertiesToAdd = new HashMap<>();
		keysToAdd.stream().forEach(key -> propertiesToAdd.put(key, addProperties.getProperty(key)));
		
		// Authentication configs
        boolean authentication = properties.getBoolean(AUTH_PROPERTY);
        if(authentication){
            String username = properties.getProperty(AUTH_USERNAME_PROPERTY);
            String password = properties.getProperty(AUTH_PASSWORD_PROPERTY);
            
            try {
                String encoding = Base64.getEncoder().encodeToString((username+":"+password).getBytes("UTF-8"));
                
                authHeader = new Header("Authorization", "Basic " + encoding);
                
                LOG.info("Authentication enabled, user: " + username);
            } catch (UnsupportedEncodingException e) {
                throw new ConfigurationException("Problem when creating authentication header");
            }
        }
	}
	
	@Override
	public void sink(Stream<AnalysisResult> outputStream) {
		Stream<JSONObject> jsonStream = outputStream.asJSON();
		jsonStream = jsonStream.map(json -> {
			for (Map.Entry<String, String> propertyToAdd : propertiesToAdd.entrySet())
				json.setProperty(propertyToAdd.getKey(), propertyToAdd.getValue());
			
			return json;
		});
		
		JavaDStream<String> jsonStringStream = jsonStream.asString().asJavaDStream();
		
		jsonStringStream.foreachRDD(rdd -> {
			List<String> jsonStringList = rdd.collect();
			
			for (String jsonString : jsonStringList)
				send(jsonString);
		});
	}

	private void send(String jsonString) throws HttpException, IOException {
        StringRequestEntity requestEntity = new StringRequestEntity(jsonString, "application/json", "UTF-8");
        PostMethod postMethod = new PostMethod(url);
        postMethod.setRequestEntity(requestEntity);
        
        if(authHeader != null)
            postMethod.setRequestHeader(authHeader);
        
        int statusCode = httpClient.executeMethod(postMethod);
        
        if (statusCode != 201) {
            throw new IOException("Unable to POST to url=" + url + " with status code=" + statusCode);
        } else {
            LOG.debug("Results posted to " + url);
        }
	}
	
	public static void setClient(HttpClient httpClient) {
		HTTPAnalysisResultSink.httpClient = httpClient;
	}

}
