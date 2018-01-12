package ch.cern.spark.json;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.util.HashMap;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.results.AnalysisResult;

public class JSONParserTest{

	public static DateTimeFormatter TIMESTAMP_FORMAT_DEFAULT = new DateTimeFormatterBuilder()
			.appendPattern(JSONParser.TIMESTAMP_OUTPUT_FORMAT)
			.toFormatter()
			.withZone(ZoneOffset.systemDefault());
	
    @Test
    public void parse() throws Exception{
        
        AnalysisResult analysisResult = new AnalysisResult();
        Instant timestamp = Instant.now();
        Map<String, String> ids = new HashMap<>();
        ids.put("id1", "val_id1");
        ids.put("id2", "val_id2");
        Metric metric = new Metric(timestamp, 12f, ids);
        analysisResult.setAnalyzedMetric(metric);
        
        HashMap<String, String> tags = new HashMap<>();
        tags.put("email", "1234@cern.ch");
        tags.put("group", "IT_DB");
		analysisResult.setTags(tags);
        
        JSONObject jsonObject = JSONParser.parse(analysisResult);
        
        Assert.assertEquals("val_id1", jsonObject.getProperty("analyzed_metric.attributes.id1"));
        Assert.assertEquals("val_id2", jsonObject.getProperty("analyzed_metric.attributes.id2"));
        
        String expected_timestamp = TIMESTAMP_FORMAT_DEFAULT.format(timestamp);
        Assert.assertEquals(expected_timestamp, jsonObject.getProperty("analyzed_metric.timestamp"));
        
        Assert.assertEquals("12.0", jsonObject.getProperty("analyzed_metric.value.num"));
        
        Assert.assertEquals("1234@cern.ch", jsonObject.getProperty("tags.email"));
        Assert.assertEquals("IT_DB", jsonObject.getProperty("tags.group"));
    }
    
    @Test
    public void parseNullValue() throws Exception{
        
        AnalysisResult analysisResult = new AnalysisResult();
        
        JSONObject jsonObject = JSONParser.parse(analysisResult);
        
        Assert.assertNull(jsonObject.getProperty("actual_value"));
    }
    
    @Test
    public void parseNull() throws Exception{
        
        JSONObject jsonObject = JSONParser.parse(null);
        
        Assert.assertNull(jsonObject);
    }
    
}
