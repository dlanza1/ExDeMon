package ch.cern.properties.source.types;

import org.apache.log4j.Logger;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.methods.GetMethod;

import com.google.gson.JsonArray;
import com.google.gson.JsonParser;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.properties.source.PropertiesSource;
import com.google.gson.*;
import java.util.Iterator;

@RegisterComponent("api")
public class ApiPropertiesSource extends PropertiesSource {
    
    private static final long serialVersionUID = -5874041352689181005L;

    private transient final static Logger LOG = Logger.getLogger(ApiPropertiesSource.class.getName());
    private String api_url;
    
    @Override
    public void config(Properties properties) throws ConfigurationException {
        api_url = properties.getProperty("url");
        
        properties.confirmAllPropertiesUsed();
    }

    @Override
    public Properties load() throws Exception {
        Properties props = new Properties();

        try {
            LOG.info("Reading from API...");
            loadSchemas(props);
            loadMetrics(props);
            loadMonitors(props);
            LOG.info("Loaded");
        } catch(Exception e) {
            LOG.error(e.getMessage(), e);
        }
        
        return props;
    }
    
    protected JsonObject loadFromUrl(String url) throws Exception {
        HttpClient client = new HttpClient();
        HttpMethod method = new GetMethod(url);
        client.executeMethod(method);
        
        if (method.getStatusCode() != 200) {
            throw new RuntimeException("Failed : HTTP error code : " + method.getStatusCode());
        }

        String output = method.getResponseBodyAsString();
        JsonParser jparser = new JsonParser();
        return jparser.parse(output).getAsJsonObject();
    }
    
    protected void loadMetrics(Properties props) throws Exception {
        JsonArray metrics = loadFromUrl(api_url + "/api/v1/metrics").getAsJsonArray("metrics");
        Iterator<JsonElement> metricsItr = metrics.iterator();
        while (metricsItr.hasNext()) {
            JsonObject metric = metricsItr.next().getAsJsonObject();
            JsonObject jobject = new JsonObject();
            jobject.add(String.format("metrics.define.%s_%s_%s", 
                                metric.get("project").getAsString(), 
                                metric.get("environment").getAsString(), 
                                metric.get("name").getAsString())
                        , metric.getAsJsonObject("data"));
            props.addFrom(jobject);
        }
    }
    
    protected void loadSchemas(Properties props) throws Exception {
        JsonArray schemas = loadFromUrl(api_url + "/api/v1/schemas").getAsJsonArray("schemas");
        Iterator<JsonElement> schemasItr = schemas.iterator();
        while (schemasItr.hasNext()) {
            JsonObject schema = schemasItr.next().getAsJsonObject();
            JsonObject jobject = new JsonObject();
            jobject.add(String.format("metrics.schema.%s_%s_%s", 
                                schema.get("project").getAsString(), 
                                schema.get("environment").getAsString(), 
                                schema.get("name").getAsString())
                        , schema.getAsJsonObject("data"));
            props.addFrom(jobject);
        }
    }
    
    protected void loadMonitors(Properties props) throws Exception {
        JsonArray monitors = loadFromUrl(api_url + "/api/v1/monitors").getAsJsonArray("monitors");
        Iterator<JsonElement> monitorsItr = monitors.iterator();
        while (monitorsItr.hasNext()) {
            JsonObject monitor = monitorsItr.next().getAsJsonObject();
            JsonObject jobject = new JsonObject();
            jobject.add(String.format("monitor.%s_%s_%s", 
                                monitor.get("project").getAsString(), 
                                monitor.get("environment").getAsString(), 
                                monitor.get("name").getAsString())
                        , monitor.getAsJsonObject("data"));
            props.addFrom(jobject);
        }
    }
}
