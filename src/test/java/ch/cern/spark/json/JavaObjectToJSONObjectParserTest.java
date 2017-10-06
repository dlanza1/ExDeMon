package ch.cern.spark.json;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.spark.JavaObjectToJSONObjectParser;
import ch.cern.spark.json.JSONObject;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;

public class JavaObjectToJSONObjectParserTest{

    @Test
    public void parse() throws Exception{
        
        AnalysisResult analysisResult = new AnalysisResult();
        Date timestamp = new Date();
        Map<String, String> ids = new HashMap<>();
        ids.put("id1", "val_id1");
        ids.put("id2", "val_id2");
        Metric metric = new Metric(timestamp, 12f, ids);
        analysisResult.setAnalyzedMetric(metric);
        
        JavaObjectToJSONObjectParser<AnalysisResult> parser = new JavaObjectToJSONObjectParser<AnalysisResult>();
        
        JSONObject jsonObject = parser.call(analysisResult);
        
        Assert.assertEquals("val_id1", jsonObject.getProperty("analyzed_metric.ids.id1"));
        Assert.assertEquals("val_id2", jsonObject.getProperty("analyzed_metric.ids.id2"));
        
        String expected_timestamp = new SimpleDateFormat(
                JavaObjectToJSONObjectParser.TIMESTAMP_OUTPUT_FORMAT).format(timestamp);
        Assert.assertEquals(expected_timestamp, jsonObject.getProperty("analyzed_metric.timestamp"));
        
        Assert.assertEquals("12.0", jsonObject.getProperty("analyzed_metric.value"));
    }
    
    @Test
    public void parseNullValue() throws Exception{
        
        AnalysisResult analysisResult = new AnalysisResult();
        
        JavaObjectToJSONObjectParser<AnalysisResult> parser = new JavaObjectToJSONObjectParser<AnalysisResult>();
        
        JSONObject jsonObject = parser.call(analysisResult);
        
        Assert.assertNull(jsonObject.getProperty("actual_value"));
    }
    
    @Test
    public void parseNull() throws Exception{
        
        JavaObjectToJSONObjectParser<AnalysisResult> parser = new JavaObjectToJSONObjectParser<AnalysisResult>();
        
        JSONObject jsonObject = parser.call(null);
        
        Assert.assertNull(jsonObject);
    }
    
}
