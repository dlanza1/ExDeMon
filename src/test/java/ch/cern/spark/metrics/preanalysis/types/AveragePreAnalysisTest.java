package ch.cern.spark.metrics.preanalysis.types;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.spark.Properties;

public class AveragePreAnalysisTest {
 
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
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
     
        Assert.assertNull(preAnalysis.getAvergaeForTime(new Date(60000)));
    }
    
    @Test
    public void average() throws Exception{
        getInstance();
        
        preAnalysis.process(new Date(20000), 10f);
        preAnalysis.process(new Date(30000), 20f);
        preAnalysis.process(new Date(40000), 30f);
        
        Float actualAverge = preAnalysis.getAvergaeForTime(new Date(60000));
        
        Assert.assertNotNull(actualAverge);
        Assert.assertEquals(20, actualAverge.floatValue(), 0);
    }
    
    @Test
    public void averageSameValue() throws Exception{
        getInstance();
        
        preAnalysis.process(new Date(20000), 100f);
        preAnalysis.process(new Date(30000), 100f);
        preAnalysis.process(new Date(40000), 100f);
        
        Float actualAverge = preAnalysis.getAvergaeForTime(new Date(60000));
        
        Assert.assertNotNull(actualAverge);
        Assert.assertEquals(100f, actualAverge.floatValue(), 0);
    }
    
    @Test
    public void averageWithOlderMetricThanPeriod() throws Exception{
        getInstance();
        
        preAnalysis.process(dateFormat.parse("2001-07-04 12:08:00"), 10f);
        preAnalysis.process(dateFormat.parse("2001-07-04 12:08:20"), 20f);
        preAnalysis.process(dateFormat.parse("2001-07-04 12:08:30"), 30f);
        
        Assert.assertEquals(25, 
                preAnalysis.getAvergaeForTime(dateFormat.parse("2001-07-04 12:08:55")), 
                0);
    }
    
}
