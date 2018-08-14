package ch.cern.exdemon.monitor.analysis.results.sink.types;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpException;
import org.junit.Test;

import com.google.gson.JsonPrimitive;

import ch.cern.exdemon.http.JsonPOSTRequest;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class HTTPAnalysisResultSinkTest {
    
    @Test
    public void shouldAddArrayOfKeys() throws ConfigurationException, HttpException, IOException, ParseException {
        Properties properties = new Properties();
        properties.setProperty("url", "https://abcd.cern.ch/<tags:url-suffix>");
        properties.setProperty("add.idb_tags", "[keys:analyzed_metric.attributes.*]");
        HTTPAnalysisResultSink sink = new HTTPAnalysisResultSink();
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
        HTTPAnalysisResultSink sink = new HTTPAnalysisResultSink();
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
