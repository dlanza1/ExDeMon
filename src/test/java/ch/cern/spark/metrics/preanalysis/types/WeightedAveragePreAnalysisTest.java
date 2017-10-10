package ch.cern.spark.metrics.preanalysis.types;

import java.time.Instant;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.spark.Properties;
import ch.cern.spark.TimeUtils;

public class WeightedAveragePreAnalysisTest {
 
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
    public void average() throws Exception{
        getInstance();
        
        int time = 40;
        
        int metric1_time = 20;
        float metric1_value = 10;
        float weight1 = (float) (default_period - (time - metric1_time)) / default_period;
        preAnalysis.process(Instant.ofEpochSecond(metric1_time), metric1_value);
        
        int metric2_time = 30;
        float metric2_value = 20;
        float weight2 = (float) (default_period - (time - metric2_time)) / default_period;
        preAnalysis.process(Instant.ofEpochSecond(metric2_time), metric2_value);
        
        int metric3_time = 40;
        float metric3_value = 30;
        float weight3 = (float) (default_period - (time - metric3_time)) / default_period;
        float avergae = preAnalysis.process(Instant.ofEpochSecond(metric3_time), metric3_value);

        float expectedValue = (metric1_value * weight1 
                             + metric2_value * weight2 
                             + metric3_value * weight3) / (weight1 + weight2 + weight3);
        
        Assert.assertEquals(expectedValue, avergae, 0);
    }
    
    @Test
    public void averageSameValue() throws Exception{
        getInstance();
        
        int metric1_time = 20;
        float metric1_value = 100;
        preAnalysis.process(Instant.ofEpochSecond(metric1_time), metric1_value);
        
        int metric2_time = 30;
        float metric2_value = 100;
        preAnalysis.process(Instant.ofEpochSecond(metric2_time), metric2_value);
        
        int metric3_time = 40;
        float metric3_value = 100;
        float avergae = preAnalysis.process(Instant.ofEpochSecond(metric3_time), metric3_value);
        
        float expectedValue = 100f;
        
        Assert.assertEquals(expectedValue, avergae, 0f);
    }
    
    @Test
    public void averageWithOlderMetricThanPeriod() throws Exception{
        getInstance();
        
        Instant time = TimeUtils.toInstant("2001-07-04 12:08:55");
        long time_in_seconds = time.getEpochSecond();
        
        preAnalysis.process(TimeUtils.toInstant("2001-07-04 12:08:00"), 10f);
        
        Instant time2 = TimeUtils.toInstant("2001-07-04 12:08:17");
        long metric2_time = time2.getEpochSecond();
        float weight2 = (float) (default_period - (time_in_seconds - metric2_time)) / default_period;
        float metric2_value = 20;
        preAnalysis.process(time2, metric2_value);
        
        Instant time3 = TimeUtils.toInstant("2001-07-04 12:08:55");
        long metric3_time = time3.getEpochSecond();
        float weight3 = (float) (default_period - (time_in_seconds - metric3_time)) / default_period;
        float metric3_value = 30;
        float avergae = preAnalysis.process(time3, metric3_value);

        float expectedValue = (metric2_value * weight2 + metric3_value * weight3) / (weight2 + weight3);
        
        Assert.assertEquals(expectedValue, avergae, 0);
    }
    
}
