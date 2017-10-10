package ch.cern.spark.metrics.preanalysis.types;

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
    public void averageFromEmptyHistory() throws Exception{
        getInstance();
     
        Assert.assertNull(preAnalysis.getAvergaeForTime(TimeUtils.toInstant(60)));
    }
    
    @Test
    public void average() throws Exception{
        getInstance();
        
        preAnalysis.process(TimeUtils.toInstant(20), 10f);
        preAnalysis.process(TimeUtils.toInstant(30), 20f);
        preAnalysis.process(TimeUtils.toInstant(40), 30f);
        
        Float actualAverge = preAnalysis.getAvergaeForTime(TimeUtils.toInstant(60));
        
        Assert.assertNotNull(actualAverge);
        Assert.assertEquals(20, actualAverge.floatValue(), 0);
    }
    
    @Test
    public void averageSameValue() throws Exception{
        getInstance();
        
        preAnalysis.process(TimeUtils.toInstant(20), 100f);
        preAnalysis.process(TimeUtils.toInstant(30), 100f);
        preAnalysis.process(TimeUtils.toInstant(40), 100f);
        
        Float actualAverge = preAnalysis.getAvergaeForTime(TimeUtils.toInstant(60));
        
        Assert.assertNotNull(actualAverge);
        Assert.assertEquals(100f, actualAverge.floatValue(), 0);
    }
    
    @Test
    public void averageWithOlderMetricThanPeriod() throws Exception{
        getInstance();
        
        preAnalysis.process(TimeUtils.toInstant("2001-07-04 12:08:00"), 10f);
        preAnalysis.process(TimeUtils.toInstant("2001-07-04 12:08:20"), 20f);
        preAnalysis.process(TimeUtils.toInstant("2001-07-04 12:08:30"), 30f);
        
        Assert.assertEquals(25, 
                preAnalysis.getAvergaeForTime(TimeUtils.toInstant("2001-07-04 12:08:55")), 
                0);
    }
    
}
