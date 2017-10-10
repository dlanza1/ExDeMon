package ch.cern.spark.metrics.preanalysis.types;

import java.time.Instant;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.spark.Properties;
import ch.cern.spark.TimeUtils;

public class AveragePreAnalysisTest {
 
    private AveragePreAnalysis preAnalysis;
    
    private void getInstance() throws Exception {
        preAnalysis = new AveragePreAnalysis();
        
        Properties properties = new Properties();
        properties.setProperty(AveragePreAnalysis.PERIOD_PARAM, "50");
        
        preAnalysis.config(properties);
        
        preAnalysis.reset();
    }
    
    @Test
    public void average() throws Exception{
        getInstance();
        
        float average = 0;
        
        average = preAnalysis.process(Instant.ofEpochSecond(20), 10f);
        average = preAnalysis.process(Instant.ofEpochSecond(30), 20f);
        average = preAnalysis.process(Instant.ofEpochSecond(40), 30f);
        
        Assert.assertEquals(20, average, 0);
    }
    
    @Test
    public void averageSameValue() throws Exception{
        getInstance();
        
        float average = 0;
        
        average = preAnalysis.process(Instant.ofEpochSecond(20), 100f);
        average = preAnalysis.process(Instant.ofEpochSecond(30), 100f);
        average = preAnalysis.process(Instant.ofEpochSecond(40), 100f);
        
        Assert.assertEquals(100f, average, 0);
    }
    
    @Test
    public void averageWithOlderMetricThanPeriod() throws Exception{
        getInstance();
        
        float average = 0;
        
        average = preAnalysis.process(TimeUtils.toInstant("2001-07-04 12:08:00"), 10f);
        average = preAnalysis.process(TimeUtils.toInstant("2001-07-04 12:08:10"), 10f);
        average = preAnalysis.process(TimeUtils.toInstant("2001-07-04 12:08:20"), 30f);
        average = preAnalysis.process(TimeUtils.toInstant("2001-07-04 12:08:30"), 30f);
        average = preAnalysis.process(TimeUtils.toInstant("2001-07-04 12:08:40"), 30f);
        average = preAnalysis.process(TimeUtils.toInstant("2001-07-04 12:08:50"), 30f);
        average = preAnalysis.process(TimeUtils.toInstant("2001-07-04 12:09:00"), 30f);
        
        Assert.assertEquals(30, average, 0);
    }
    
}
