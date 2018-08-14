package ch.cern.exdemon.http;

import java.io.IOException;
import java.io.Serializable;
import java.text.ParseException;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.http.HttpException;
import org.apache.http.HttpResponse;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.HttpClient;
import org.apache.http.client.HttpRequestRetryHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.auth.BasicScheme;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.client.StandardHttpRequestRetryHandler;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.json.JSON;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import lombok.ToString;

@ToString(callSuper=false)
public class HTTPSink implements Serializable{
	
	private static final long serialVersionUID = 2779022310649799825L;

	private final static Logger LOG = LogManager.getLogger(HTTPSink.class);

	private static HttpClient httpClient;

	public static final String URL_PARAM = "url";
	private String url;
	
	public static final String RETRIES_PARAM = "retries";
	private static HttpRequestRetryHandler retryHandler;
	
	public static final String TIMEOUT_PARAM = "timeout";
	private int timeout_ms;
	
	public static final String PARALLELIZATION_PARAM = "parallelization";
	private int parallelization;
	
	public static final String BATCH_SIZE_PARAM = "batch.size";
	private int batch_size;
	
	public static final String AUTH_PARAM = "auth";
	public static final String AUTH_TYPE_PARAM = AUTH_PARAM + ".type";
    public static final String AUTH_USERNAME_PARAM = AUTH_PARAM + ".user";
    public static final String AUTH_PASSWORD_PARAM = AUTH_PARAM + ".password";
    private UsernamePasswordCredentials authCredentials;
	
	private static final String AS_ARRAY_PARAM = "as-array";
    private boolean as_array;

    private boolean addAction;

	public ConfigurationResult config(Properties properties) {
	    ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
	    
		url = properties.getProperty(URL_PARAM);
		if(url == null)
		    confResult.withMustBeConfigured(URL_PARAM);
		
		int retries = (int) properties.getFloat(RETRIES_PARAM, 1);
		retryHandler = new StandardHttpRequestRetryHandler(retries, false);
		
		timeout_ms = (int) properties.getFloat(TIMEOUT_PARAM, 5000);
		parallelization = (int) properties.getFloat(PARALLELIZATION_PARAM, 1);
		batch_size = (int) properties.getFloat(BATCH_SIZE_PARAM, 100);
		try {
            as_array = properties.getBoolean(AS_ARRAY_PARAM, true);
        } catch (ConfigurationException e) {
            confResult.withError(null, e);
        }
		
		String authenticationType = properties.getProperty(AUTH_TYPE_PARAM, "disabled");
        if(authenticationType.equals("basic-user-password")){
            String username = properties.getProperty(AUTH_USERNAME_PARAM);
            String password = properties.getProperty(AUTH_PASSWORD_PARAM);
            
            authCredentials = new UsernamePasswordCredentials(username, password);
        }else if(authenticationType.equals("disabled")){
            authCredentials = null;
        }else {
            confResult.withError(AUTH_TYPE_PARAM, "authentication type \"" + authenticationType + "\" is not available");
        }
        
        return confResult;
	}
	
	public void sink(JavaDStream<JsonPOSTRequest> jsonRequests) {
	    jsonRequests = jsonRequests.repartition(parallelization);
	    
	    jsonRequests.foreachRDD(rdd -> rdd.foreachPartitionAsync(requests -> batchAndSend(requests)));
	}

	public void sink(JsonPOSTRequest request) throws ParseException {
        batchAndSend(Collections.singleton(request).iterator());
    }
    
    protected void batchAndSend(Iterator<JsonPOSTRequest> requests) {
        List<JsonPOSTRequest> requestsToSend = new LinkedList<>();
        while (requests.hasNext()) {
            JsonPOSTRequest request = requests.next();
            
            request.setUrlIfNull(url);
            
            requestsToSend.add(request);
            
            if(requestsToSend.size() >= batch_size) {
                buildBatchAndSend(requestsToSend);
                
                requestsToSend = new LinkedList<>();
            }
        }
        
        buildBatchAndSend(requestsToSend);
    }

    public void buildBatchAndSend(List<JsonPOSTRequest> requests) {
	    if(as_array)
	        requests = buildJSONArrays(requests);

        for (JsonPOSTRequest request : requests)
            trySend(request);
    }

    private static HttpClient getHTTPClient() {
        if(HTTPSink.httpClient == null)
            HTTPSink.httpClient = HttpClients.custom()
                                             .setRetryHandler(retryHandler)
                                             .setConnectionTimeToLive(1, TimeUnit.MINUTES)
                                             .setMaxConnPerRoute(1000)
                                             .setMaxConnTotal(10000)
                                             .build();
        
		return HTTPSink.httpClient;
	}
	
	public static HttpClient setHTTPClient(HttpClient httpClient) {
		return HTTPSink.httpClient = httpClient;
	}

	public void trySend(JsonPOSTRequest request) {		
		HttpClient httpClient = getHTTPClient();
		
        try {
            send(httpClient, request);
        } catch (Exception e) {
            LOG.error("Error sending request: " + request, e);
        }
	}
	
	private void send(HttpClient httpClient, JsonPOSTRequest request) throws HttpException, IOException {
        HttpPost postMethod = request.toPostMethod();
        
        postMethod.setConfig(RequestConfig.custom()
                                                .setConnectTimeout(timeout_ms)
                                                .setSocketTimeout(timeout_ms)
                                                .setConnectionRequestTimeout(timeout_ms)
                                                .build());
        
        if(authCredentials != null)
            postMethod.addHeader(new BasicScheme().authenticate(authCredentials, postMethod, null));
        
		HttpResponse response = null;
        try {
            response = httpClient.execute(postMethod);
        } catch (IOException e) {
            try{
                postMethod.abort();
                postMethod.completed();
            }catch(Exception ee){}
            
            throw new HttpException("Unable to POST to url=" + request.getUrl(), e);
        }finally {
            postMethod.releaseConnection();
        }
		
		int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == 201 || statusCode == 200) {
            LOG.trace("JSON: " + request.getJson() + " sent to " + request.getUrl());
        } else {
            throw new HttpException("Unable to POST to url=" + request.getUrl() + " with status code=" + statusCode + " "+response.toString()+". JSON: " + request.getJson());
        }
	}

    private List<JsonPOSTRequest> buildJSONArrays(List<JsonPOSTRequest> requests) {
	    Map<String, List<JsonPOSTRequest>> groupedByUrl = requests.stream().collect(Collectors.groupingBy(JsonPOSTRequest::getUrl));
	    
	    return groupedByUrl.entrySet().stream().map(entry -> {
            	        String jsonString = entry.getValue().stream().map(req -> req.getJson().toString()).collect(Collectors.toList()).toString();
            	        
            	        return new JsonPOSTRequest(entry.getKey(), new JSON(jsonString));
            	    }).collect(Collectors.toList());
	}

}
