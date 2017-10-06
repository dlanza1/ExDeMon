package ch.cern.spark.metrics.analysis.types;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.spark.Properties;
import ch.cern.spark.metrics.results.AnalysisResult;

public class RecentActivityAnalysisTest {

    @Test
    public void parse() throws Exception{
        RecentActivityAnalysis analysis = new RecentActivityAnalysis();
        
        Properties properties = new Properties();
        
        analysis.config(properties);

        assertEquals(RecentActivityAnalysis.PERIOD_DEFAULT, analysis.getPeriod_in_seconds());
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
        
        assertEquals(180, analysis.getPeriod_in_seconds());
        assertEquals(3.1f, analysis.getWarn_ratio(), 0f);
        assertEquals(3.2f, analysis.getError_ratio(), 0f);
        assertTrue(analysis.isError_lowerbound());
        assertTrue(analysis.isWarning_upperbound());
        assertTrue(analysis.isWarning_lowerbound());
        assertTrue(analysis.isError_upperbound());
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
        
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(1000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(new Date(2000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(3000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(4000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(5000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(6000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(7000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(8000), 40f).getStatus());
        // Average  35
        // Variance 5.3
        // WARNING UP = 35 + 2 * 5.3 = 45.6
        
        Assert.assertEquals(AnalysisResult.Status.WARNING,  analysis.process(new Date(9000), 46f).getStatus());
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
        
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(1000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(new Date(2000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(3000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(4000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(5000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(6000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(7000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(8000), 40f).getStatus());
        // Average  35
        // Variance 5.3
        // WARNING LOW = 35 - 2 * 5.3 = 24.4
        
        Assert.assertEquals(AnalysisResult.Status.WARNING,  analysis.process(new Date(9000), 24f).getStatus());
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
        
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(1000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(new Date(2000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(3000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(4000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(5000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(6000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(7000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(8000), 40f).getStatus());
        // Average  35
        // Variance 5.3
        // ERROR   UP = 35 + 3 * 5.3 = 50.9
        
        Assert.assertEquals(AnalysisResult.Status.ERROR,  analysis.process(new Date(9000), 52f).getStatus());
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
        
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(1000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR,    analysis.process(new Date(2000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(3000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(4000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(5000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(6000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(7000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(8000), 40f).getStatus());
        // Average  35
        // Variance 5.3
        // ERROR   LOW = 35 - 3 * 5.3 = 19
        
        Assert.assertEquals(AnalysisResult.Status.ERROR,  analysis.process(new Date(9000), 18f).getStatus());
    }
    
}
