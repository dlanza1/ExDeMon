package ch.cern.exdemon.monitor.analysis.results.sink.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.text.ParseException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import org.junit.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonPrimitive;

import ch.cern.exdemon.http.JsonPOSTRequest;
import ch.cern.exdemon.metrics.Metric;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.properties.Properties;

public class CERNAnalysisResultSinkTest {

    @Test
    public void addInfluxDBTags() throws ParseException {
        CERNAnalysisResultSink sink = new CERNAnalysisResultSink();
        Properties properties = new Properties();
        properties.setProperty("url", "https://abcd.cern.ch/<tags:url-suffix>");
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
        tags.put("influx.tags.attributes", "att1 att2 att4");
        analysis.setTags(tags);
        
        JsonPOSTRequest request = sink.toJsonPOSTRequest(analysis);
        
        assertEquals("value1", request.getJson().getProperty("idbtags.$value"));
        assertEquals("att1-value", request.getJson().getProperty("idbtags.att1"));
        assertEquals("att2-value", request.getJson().getProperty("idbtags.att2"));
        assertNull(request.getJson().getProperty("idbtags.att3"));
        assertNull(request.getJson().getProperty("idbtags.att4"));
        
        JsonArray expectedTagKeys = new JsonArray();
        expectedTagKeys.add(new JsonPrimitive("idbtags.$value"));
        expectedTagKeys.add(new JsonPrimitive("idbtags.att1"));
        expectedTagKeys.add(new JsonPrimitive("idbtags.att2"));
        assertEquals(expectedTagKeys, request.getJson().getElement("idb_tags"));
    }
    
}
