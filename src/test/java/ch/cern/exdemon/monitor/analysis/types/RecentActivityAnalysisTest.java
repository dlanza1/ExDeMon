package ch.cern.exdemon.monitor.analysis.types;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.properties.Properties;

public class RecentActivityAnalysisTest {

    @Test
    public void parse() throws Exception{
        RecentActivityAnalysis analysis = new RecentActivityAnalysis();
        
        Properties properties = new Properties();
        
        analysis.config(properties);

        assertEquals(RecentActivityAnalysis.PERIOD_DEFAULT, analysis.getPeriod());
        assertEquals(RecentActivityAnalysis.WARN_RATIO_DEFAULT, analysis.getWarn_ratio(), 0f);
        assertEquals(RecentActivityAnalysis.ERROR_RATIO_DEFAULT, analysis.getError_ratio(), 0f);
        assertFalse(analysis.isError_lowerbound());
        assertFalse(analysis.isWarning_upperbound());
        assertFalse(analysis.isWarning_lowerbound());
        assertFalse(analysis.isError_upperbound());
        
        properties.put(RecentActivityAnalysis.PERIOD_PARAM, "3m");
        properties.put(RecentActivityAnalysis.WARN_RATIO_PARAM, "3.1");
        properties.put(RecentActivityAnalysis.ERROR_RATIO_PARAM, "3.2");
        properties.put(RecentActivityAnalysis.ERROR_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.ERROR_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        analysis.config(properties);
        
        assertEquals(180, analysis.getPeriod().getSeconds());
        assertEquals(3.1f, analysis.getWarn_ratio(), 0f);
        assertEquals(3.2f, analysis.getError_ratio(), 0f);
        assertTrue(analysis.isError_lowerbound());
        assertTrue(analysis.isWarning_upperbound());
        assertTrue(analysis.isWarning_lowerbound());
        assertTrue(analysis.isError_upperbound());
    }
    
    @Test
    public void notLearning() throws Exception{
        RecentActivityAnalysis analysis = new RecentActivityAnalysis();
        
        Properties properties = new Properties();
        properties.put(RecentActivityAnalysis.PERIOD_PARAM, "30s");
        properties.put(RecentActivityAnalysis.WARN_RATIO_PARAM, "2");
        properties.put(RecentActivityAnalysis.ERROR_RATIO_PARAM, "3");
        properties.put(RecentActivityAnalysis.LEARNING_UPPERBOUND_RATIO_PARAM, "3");
        properties.put(RecentActivityAnalysis.LEARNING_LOWERBOUND_RATIO_PARAM, "3");
        properties.put(RecentActivityAnalysis.ERROR_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.ERROR_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        analysis.config(properties);
        
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(Instant.ofEpochSecond(1), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(Instant.ofEpochSecond(2), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(3), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(4), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(5), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(6), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(7), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(8), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(9), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(10), 40f).getStatus());
        // Average  35
        // Variance 5.3
        
        //Not learnt
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(Instant.ofEpochSecond(11), 1000f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(Instant.ofEpochSecond(12), -1000f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(Instant.ofEpochSecond(13), 1000f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(Instant.ofEpochSecond(14), -1000f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(Instant.ofEpochSecond(15), 1000f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(Instant.ofEpochSecond(16), -1000f).getStatus());
        
        // WARNING UP = 35 + 2 * 5.3 = 45.6
        Assert.assertEquals(AnalysisResult.Status.WARNING,  analysis.process(Instant.ofEpochSecond(17), 46f).getStatus());
    }
    
    @Test
    public void learningRatio() throws Exception{
        RecentActivityAnalysis analysis = new RecentActivityAnalysis();
        
        Properties properties = new Properties();
        properties.put(RecentActivityAnalysis.LEARNING_RATIO_PARAM, "0.1");
        properties.put(RecentActivityAnalysis.WARN_RATIO_PARAM, "2");
        properties.put(RecentActivityAnalysis.ERROR_RATIO_PARAM, "3");
        properties.put(RecentActivityAnalysis.ERROR_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.ERROR_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        analysis.config(properties);
        
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(Instant.now(), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(Instant.now(), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.now(), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.now(), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.now(), 30f).getStatus());
    }
    
    @Test
    public void learningRatioStartWith0Variance() throws Exception{
        RecentActivityAnalysis analysis = new RecentActivityAnalysis();
        
        Properties properties = new Properties();
        properties.put(RecentActivityAnalysis.LEARNING_RATIO_PARAM, "0.1");
        properties.put(RecentActivityAnalysis.WARN_RATIO_PARAM, "2");
        properties.put(RecentActivityAnalysis.ERROR_RATIO_PARAM, "3");
        properties.put(RecentActivityAnalysis.ERROR_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.ERROR_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        analysis.config(properties);
        
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(Instant.now(), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.now(), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.now(), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.now(), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(Instant.now(), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.now(), 40f).getStatus());
    }
    
    @Test
    public void analysisWarningUP() throws Exception{
        RecentActivityAnalysis analysis = new RecentActivityAnalysis();
        
        Properties properties = new Properties();
        properties.put(RecentActivityAnalysis.PERIOD_PARAM, "30s");
        properties.put(RecentActivityAnalysis.WARN_RATIO_PARAM, "2");
        properties.put(RecentActivityAnalysis.ERROR_RATIO_PARAM, "3");
        properties.put(RecentActivityAnalysis.ERROR_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.ERROR_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        analysis.config(properties);
        
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(Instant.ofEpochSecond(1), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(Instant.ofEpochSecond(2), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(3), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(4), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(5), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(6), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(7), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(8), 40f).getStatus());
        // Average  35
        // Variance 5.3
        // WARNING UP = 35 + 2 * 5.3 = 45.6
        
        Assert.assertEquals(AnalysisResult.Status.WARNING,  analysis.process(Instant.ofEpochSecond(9), 46f).getStatus());
    }

    @Test
    public void analysisWarningLow() throws Exception{
        RecentActivityAnalysis analysis = new RecentActivityAnalysis();
        
        Properties properties = new Properties();
        properties.put(RecentActivityAnalysis.PERIOD_PARAM, "30s");
        properties.put(RecentActivityAnalysis.WARN_RATIO_PARAM, "2");
        properties.put(RecentActivityAnalysis.ERROR_RATIO_PARAM, "3");
        properties.put(RecentActivityAnalysis.ERROR_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.ERROR_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        analysis.config(properties);
        
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(Instant.ofEpochSecond(1), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(Instant.ofEpochSecond(2), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(3), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(4), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(5), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(6), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(7), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(8), 40f).getStatus());
        // Average  35
        // Variance 5.3
        // WARNING LOW = 35 - 2 * 5.3 = 24.4
        
        Assert.assertEquals(AnalysisResult.Status.WARNING,  analysis.process(Instant.ofEpochSecond(9), 24f).getStatus());
    }
    
    @Test
    public void analysisErrorUP() throws Exception{
        RecentActivityAnalysis analysis = new RecentActivityAnalysis();
        
        Properties properties = new Properties();
        properties.put(RecentActivityAnalysis.PERIOD_PARAM, "30s");
        properties.put(RecentActivityAnalysis.WARN_RATIO_PARAM, "2");
        properties.put(RecentActivityAnalysis.ERROR_RATIO_PARAM, "3");
        properties.put(RecentActivityAnalysis.ERROR_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.ERROR_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        analysis.config(properties);
        
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(Instant.ofEpochSecond(1), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(Instant.ofEpochSecond(2), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(3), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(4), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(5), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(6), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(7), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(8), 40f).getStatus());
        // Average  35
        // Variance 5.3
        // ERROR   UP = 35 + 3 * 5.3 = 50.9
        
        Assert.assertEquals(AnalysisResult.Status.ERROR,  analysis.process(Instant.ofEpochSecond(9), 52f).getStatus());
    }
    
    @Test
    public void analysisErrorLow() throws Exception{
        RecentActivityAnalysis analysis = new RecentActivityAnalysis();
        
        Properties properties = new Properties();
        properties.put(RecentActivityAnalysis.PERIOD_PARAM, "30s");
        properties.put(RecentActivityAnalysis.WARN_RATIO_PARAM, "2");
        properties.put(RecentActivityAnalysis.ERROR_RATIO_PARAM, "3");
        properties.put(RecentActivityAnalysis.ERROR_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.WARNING_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(RecentActivityAnalysis.ERROR_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        analysis.config(properties);
        
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(Instant.ofEpochSecond(1), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(Instant.ofEpochSecond(2), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(3), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(4), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(5), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(6), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(7), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(Instant.ofEpochSecond(8), 40f).getStatus());
        // Average  35
        // Variance 5.3
        // ERROR   LOW = 35 - 3 * 5.3 = 19
        
        Assert.assertEquals(AnalysisResult.Status.ERROR,  analysis.process(Instant.ofEpochSecond(9), 18f).getStatus());
    }
    
}
