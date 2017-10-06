package ch.cern.spark.metrics.preanalysis.types;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.spark.Properties;

public class WeightedAveragePreAnalysisTest {
 
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    private WeightedAveragePreAnalysis preAnalysis;

    private int default_period = 50;
    
    private void getInstance() throws Exception {
        preAnalysis = new WeightedAveragePreAnalysis();
        
        Properties properties = new Properties();
        properties.setProperty(AveragePreAnalysis.PERIOD_PARAM, default_period+"");
        
        preAnalysis.config(properties);
        
        preAnalysis.reset();
    }
    
    @Test
    public void averageFromEmptyHistory() throws Exception{
        getInstance();
     
        Assert.assertNull(preAnalysis.getAvergaeForTime(new Date(60000)));
    }
    
    @Test
    public void average() throws Exception{
        getInstance();
        
        long time = 60;
        
        int metric1_time = 20;
        float metric1_value = 10;
        float weight1 = (float) (default_period - (time - metric1_time)) / default_period;
        preAnalysis.process(new Date(metric1_time * 1000), metric1_value);
        
        int metric2_time = 30;
        float metric2_value = 20;
        float weight2 = (float) (default_period - (time - metric2_time)) / default_period;
        preAnalysis.process(new Date(metric2_time * 1000), metric2_value);
        
        int metric3_time = 40;
        float metric3_value = 30;
        float weight3 = (float) (default_period - (time - metric3_time)) / default_period;
        preAnalysis.process(new Date(metric3_time * 1000), metric3_value);
        
        Float actualAverge = preAnalysis.getAvergaeForTime(new Date(time * 1000));
        
        float expectedValue = (metric1_value * weight1 
                             + metric2_value * weight2 
                             + metric3_value * weight3) / (weight1 + weight2 + weight3);
        
        Assert.assertNotNull(actualAverge);
        Assert.assertEquals(expectedValue, actualAverge.floatValue(), 0);
    }
    
    @Test
    public void averageSameValue() throws Exception{
        getInstance();
        
        long time = 60;
        
        int metric1_time = 20;
        float metric1_value = 100;
        preAnalysis.process(new Date(metric1_time * 1000), metric1_value);
        
        int metric2_time = 30;
        float metric2_value = 100;
        preAnalysis.process(new Date(metric2_time * 1000), metric2_value);
        
        int metric3_time = 40;
        float metric3_value = 100;
        preAnalysis.process(new Date(metric3_time * 1000), metric3_value);
        
        Float actualAverge = preAnalysis.getAvergaeForTime(new Date(time * 1000));
        
        float expectedValue = 100f;
        
        Assert.assertEquals(expectedValue, actualAverge.floatValue(), 0f);
    }
    
    @Test
    public void averageWithOlderMetricThanPeriod() throws Exception{
        getInstance();
        
        Date time = dateFormat.parse("2001-07-04 12:08:55");
        long time_in_seconds = time.getTime() / 1000;
        
        preAnalysis.process(dateFormat.parse("2001-07-04 12:08:00"), 10f);
        
        Date time2 = dateFormat.parse("2001-07-04 12:08:17");
        long metric2_time = time2.getTime() / 1000;
        float weight2 = (float) (default_period - (time_in_seconds - metric2_time)) / default_period;
        float metric2_value = 20;
        preAnalysis.process(time2, metric2_value);
        
        Date time3 = dateFormat.parse("2001-07-04 12:08:30");
        long metric3_time = time3.getTime() / 1000;
        float weight3 = (float) (default_period - (time_in_seconds - metric3_time)) / default_period;
        float metric3_value = 30;
        preAnalysis.process(time3, metric3_value);

        float expectedValue = (metric2_value * weight2 + metric3_value * weight3) / (weight2 + weight3);
        
        Float actualValue = preAnalysis.getAvergaeForTime(time);
        Assert.assertNotNull(actualValue);
        Assert.assertEquals(expectedValue, actualValue, 0);
    }
    
}
