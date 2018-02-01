package ch.cern.spark.http;

import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.time.Duration;
import java.util.Base64;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.stream.Collectors;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.Taggable;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.json.JSONObject;
import ch.cern.spark.json.JSONParser;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notifications.sink.NotificationsSink;
import ch.cern.utils.TimeUtils;

public class HTTPSink implements Serializable{
	
	private static final long serialVersionUID = 2779022310649799825L;

	private final static Logger LOG = LogManager.getLogger(HTTPSink.class);

	private static HttpClient httpClient;

	public static final String URL_PARAM = "url";
	private String url;
	
	public static final String RETRIES_PARAM = "retries";
	private int retries;
	
	public static final String TIMEOUT_PARAM = "timeout";
	private int timeout_ms;
	
	public static final String PARALLELIZATION_PARAM = "parallelization";
	private int parallelization;
	
	public static final String BATCH_SIZE_PARAM = "batch.size";
	private int batch_size;
	
	public static final String AUTH_PARAM = "auth";
    public static final String AUTH_USERNAME_PARAM = AUTH_PARAM + ".user";
    public static final String AUTH_PASSWORD_PARAM = AUTH_PARAM + ".password";
    private Header authHeader;
    
	private Map<String, String> propertiesToAdd;
	
	private static final String AS_ARRAY_PARAM = "as-array";
    private boolean as_array;

    private boolean addNotification;

	public void config(Properties properties) throws ConfigurationException {
		url = properties.getProperty(URL_PARAM);
		if(url == null)
		    throw new ConfigurationException("URL must be specified");
		
		retries = (int) properties.getFloat(RETRIES_PARAM, 1);
		timeout_ms = (int) properties.getFloat(TIMEOUT_PARAM, 2000);
		parallelization = (int) properties.getFloat(PARALLELIZATION_PARAM, 1);
		batch_size = (int) properties.getFloat(BATCH_SIZE_PARAM, 100);
		as_array = properties.getBoolean(AS_ARRAY_PARAM, true);
		
		addNotification = properties.getBoolean("add.$notification", true);
        
		propertiesToAdd = properties.getSubset("add").toStringMap();
		propertiesToAdd.remove("add.$notification");
		
		// Authentication configs
        boolean authentication = properties.getBoolean(AUTH_PARAM);
        if(authentication){
            String username = properties.getProperty(AUTH_USERNAME_PARAM);
            String password = properties.getProperty(AUTH_PASSWORD_PARAM);
            
            try {
                String encoding = Base64.getEncoder().encodeToString((username+":"+password).getBytes("UTF-8"));
                
                authHeader = new Header("Authorization", "Basic " + encoding);
                
                LOG.info("Authentication enabled, user: " + username);
            } catch (UnsupportedEncodingException e) {
                throw new ConfigurationException("Problem when creating authentication header");
            }
        }
	}
	
	public void sink(JavaDStream<?> outputStream) {
		outputStream = outputStream.repartition(parallelization);
		
		JavaDStream<JsonPOSTRequest> requestsStream = outputStream.map(object -> toJsonPOSTRequest(object));
		
		requestsStream.foreachRDD(rdd -> rdd.foreachPartition(strings -> send(strings)));
	}

    protected JsonPOSTRequest toJsonPOSTRequest(Object object) throws ParseException {
        Map<String, String> tags = null;
        if(object instanceof Taggable)
            tags = ((Taggable) object).getTags();
        
        String url = this.url;
        if(object instanceof Notification)
            url = NotificationsSink.template(url, (Notification) object);
        
        JSONObject json = addNotification ? JSONParser.parse(object) : new JSONObject("{}");
        
        JsonPOSTRequest request = new JsonPOSTRequest(url, json);
        
        for (Map.Entry<String, String> propertyToAdd : propertiesToAdd.entrySet()) {
            String value = propertyToAdd.getValue();
            
            if(value.startsWith("%"))
                if(tags != null)
                    value = tags.get(value.substring(1));
                else
                    value = null;
            
            if(value != null && object instanceof Notification)
                value = NotificationsSink.template(value, (Notification) object);
            
            request.addProperty(propertyToAdd.getKey(), value);
        }
        
        return request;
    }
    
    protected void send(Iterator<JsonPOSTRequest> requests) {
        List<Exception> thrownExceptions = new LinkedList<>();
        Exception thrownException = null;
        
        List<JsonPOSTRequest> requestsToSend = new LinkedList<>();
        while (requests.hasNext()) {
            requestsToSend.add(requests.next());
            
            if(requestsToSend.size() >= batch_size) {
                thrownException = sendBatch(requestsToSend);
                if(thrownException != null)
                    thrownExceptions.add(thrownException);
                
                requestsToSend = new LinkedList<>();
            }
        }
        
        thrownException = sendBatch(requestsToSend);
        if(thrownException != null)
            thrownExceptions.add(thrownException);
        
        if(!thrownExceptions.isEmpty())
            LOG.error(new IOException("Same batches could not be sent. Exceptions: " + thrownExceptions));
    }

    private Exception sendBatch(List<JsonPOSTRequest> requests) {
	    if(as_array)
	        requests = buildJSONArrays(requests);

        for (JsonPOSTRequest request : requests) {
            Exception excep = send(request);
            
            if(excep != null)
                return excep;
        }
	    
	    return null;
    }

    private static HttpClient getHTTPClient() {
		return HTTPSink.httpClient == null ? new HttpClient() : HTTPSink.httpClient;
	}
	
	public static HttpClient setHTTPClient(HttpClient httpClient) {
		return HTTPSink.httpClient = httpClient;
	}

	private Exception send(JsonPOSTRequest request) {		
		HttpClient httpClient = getHTTPClient();
		
		Exception thrownException = new Exception();
		
		for (int i = 0; i < retries && thrownException != null; i++) {
			try {
				trySend(httpClient, request);
				
				thrownException = null;
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				
				thrownException = e;
				
				setHTTPClient(null);
				httpClient = getHTTPClient();
			}	
		}
		
		return thrownException;	
	}
	
	private void trySend(HttpClient httpClient, JsonPOSTRequest request) throws HttpException, IOException {
        PostMethod postMethod = request.toPostMethod();
        
        if(authHeader != null)
            postMethod.setRequestHeader(authHeader);
        
        TimerTask task = new TimerTask() {
            @Override
            public void run() {
                if (postMethod != null)
                		postMethod.abort();
            }
        };
        new Timer(true).schedule(task, timeout_ms);
        
		int statusCode = httpClient.executeMethod(postMethod);
		
		if(postMethod.isAborted())
			throw new HttpException("Request has timmed out after " + TimeUtils.toString(Duration.ofMillis(timeout_ms)));
		
        if (statusCode != 201 && statusCode != 200) {
        		throw new HttpException("Unable to POST to url=" + request.getUrl() + " with status code=" + statusCode);
        } else {
            LOG.trace("JSON: " + request.getJson() + " sent to " + request.getUrl());
        }
	}

	private List<JsonPOSTRequest> buildJSONArrays(List<JsonPOSTRequest> elements) {
	    Map<String, List<JsonPOSTRequest>> groupedByUrl = elements.stream().collect(Collectors.groupingBy(JsonPOSTRequest::getUrl));
	    
	    return groupedByUrl.entrySet().stream().map(entry -> {
            	        String jsonString = entry.getValue().stream().map(req -> req.getJson().toString()).collect(Collectors.toList()).toString();
            	        
            	        return new JsonPOSTRequest(entry.getKey(), new JSONObject(jsonString));
            	    }).collect(Collectors.toList());
	}

}
