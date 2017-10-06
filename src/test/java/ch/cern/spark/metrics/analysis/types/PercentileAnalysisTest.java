package ch.cern.spark.metrics.analysis.types;

import static org.junit.Assert.*;

import java.util.Date;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.spark.Properties;
import ch.cern.spark.metrics.results.AnalysisResult;

public class PercentileAnalysisTest {

    @Test
    public void parse() throws Exception{
        PercentileAnalysis analysis = new PercentileAnalysis();
        
        Properties properties = new Properties();
        
        analysis.config(properties);

        assertEquals(PercentileAnalysis.PERIOD_DEFAULT, analysis.getPeriod_in_seconds());
        assertEquals(PercentileAnalysis.WARN_RATIO_DEFAULT, analysis.getWarn_ratio(), 0f);
        assertEquals(PercentileAnalysis.WARN_PERCENTILE_DEFAULT, analysis.getWarn_percentile(), 0f);
        assertEquals(PercentileAnalysis.ERROR_RATIO_DEFAULT, analysis.getError_ratio(), 0f);
        assertEquals(PercentileAnalysis.ERROR_PERCENTILE_DEFAULT, analysis.getError_percentile(), 0f);
        assertFalse(analysis.isError_lowerbound());
        assertFalse(analysis.isWarning_upperbound());
        assertFalse(analysis.isWarning_lowerbound());
        assertFalse(analysis.isError_upperbound());
        
        properties.put(PercentileAnalysis.PERIOD_PARAM, "3m");
        properties.put(PercentileAnalysis.WARN_RATIO_PARAM, "3.1");
        properties.put(PercentileAnalysis.WARN_PERCENTILE_PARAM, "60");
        properties.put(PercentileAnalysis.ERROR_RATIO_PARAM, "3.2");
        properties.put(PercentileAnalysis.ERROR_PERCENTILE_PARAM, "80");
        properties.put(PercentileAnalysis.ERROR_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(PercentileAnalysis.WARNING_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(PercentileAnalysis.WARNING_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(PercentileAnalysis.ERROR_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        analysis.config(properties);
        
        assertEquals(180, analysis.getPeriod_in_seconds());
        assertEquals(3.1f, analysis.getWarn_ratio(), 0f);
        assertEquals(3.2f, analysis.getError_ratio(), 0f);
        assertEquals(60, analysis.getWarn_percentile(), 0f);
        assertEquals(80, analysis.getError_percentile(), 0f);
        assertTrue(analysis.isError_lowerbound());
        assertTrue(analysis.isWarning_upperbound());
        assertTrue(analysis.isWarning_lowerbound());
        assertTrue(analysis.isError_upperbound());
    }
    
    @Test
    public void analysisWarningUP() throws Exception{
        PercentileAnalysis analysis = new PercentileAnalysis();
        
        Properties properties = new Properties();
        properties.put(PercentileAnalysis.PERIOD_PARAM, "3m");
        properties.put(PercentileAnalysis.WARN_RATIO_PARAM, "2");
        properties.put(PercentileAnalysis.WARN_PERCENTILE_PARAM, "60");
        properties.put(PercentileAnalysis.ERROR_RATIO_PARAM, "3");
        properties.put(PercentileAnalysis.ERROR_PERCENTILE_PARAM, "80");
        properties.put(PercentileAnalysis.ERROR_UPPERBOUND_PARAM, Boolean.FALSE.toString());
        properties.put(PercentileAnalysis.WARNING_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(PercentileAnalysis.WARNING_LOWERBOUND_PARAM, Boolean.FALSE.toString());
        properties.put(PercentileAnalysis.ERROR_LOWERBOUND_PARAM, Boolean.FALSE.toString());
        analysis.config(properties);
        
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(1000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(2000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(3000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(4000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.WARNING,  analysis.process(new Date(5000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(6000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(7000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(8000), 35f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(9000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(10000), 30f).getStatus());
        // Mean  30
        // Percentile 60 = 33
        // WARNING UP = 33 + 2 * diff(30 - 33) = 39
        
        Assert.assertEquals(AnalysisResult.Status.WARNING,  analysis.process(new Date(9000), 40f).getStatus());
    }

    @Test
    public void analysisWarningLow() throws Exception{
        PercentileAnalysis analysis = new PercentileAnalysis();
        
        Properties properties = new Properties();
        properties.put(PercentileAnalysis.PERIOD_PARAM, "3m");
        properties.put(PercentileAnalysis.WARN_RATIO_PARAM, "2");
        properties.put(PercentileAnalysis.WARN_PERCENTILE_PARAM, "60");
        properties.put(PercentileAnalysis.ERROR_RATIO_PARAM, "3");
        properties.put(PercentileAnalysis.ERROR_PERCENTILE_PARAM, "80");
        properties.put(PercentileAnalysis.ERROR_UPPERBOUND_PARAM, Boolean.FALSE.toString());
        properties.put(PercentileAnalysis.WARNING_UPPERBOUND_PARAM, Boolean.FALSE.toString());
        properties.put(PercentileAnalysis.WARNING_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(PercentileAnalysis.ERROR_LOWERBOUND_PARAM, Boolean.FALSE.toString());
        analysis.config(properties);
        
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(1000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(2000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(3000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(4000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(5000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(6000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(7000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(8000), 35f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(9000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(10000), 30f).getStatus());
        // Median  30
        // Percentile 40 = 30
        // WARNING LOW = 30 - 2 * diff(30 - 30) = 30
        
        Assert.assertEquals(AnalysisResult.Status.WARNING,  analysis.process(new Date(9000), 29f).getStatus());
    }
    
    @Test
    public void analysisErrorUP() throws Exception{
        PercentileAnalysis analysis = new PercentileAnalysis();
        
        Properties properties = new Properties();
        properties.put(PercentileAnalysis.PERIOD_PARAM, "3m");
        properties.put(PercentileAnalysis.WARN_RATIO_PARAM, "2");
        properties.put(PercentileAnalysis.WARN_PERCENTILE_PARAM, "60");
        properties.put(PercentileAnalysis.ERROR_RATIO_PARAM, "3");
        properties.put(PercentileAnalysis.ERROR_PERCENTILE_PARAM, "80");
        properties.put(PercentileAnalysis.ERROR_UPPERBOUND_PARAM, Boolean.TRUE.toString());
        properties.put(PercentileAnalysis.WARNING_UPPERBOUND_PARAM, Boolean.FALSE.toString());
        properties.put(PercentileAnalysis.WARNING_LOWERBOUND_PARAM, Boolean.FALSE.toString());
        properties.put(PercentileAnalysis.ERROR_LOWERBOUND_PARAM, Boolean.FALSE.toString());
        analysis.config(properties);
        
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(1000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(2000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(3000), 35f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(4000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(5000), 35f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(6000), 35f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(7000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(8000), 35f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(9000), 35f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(10000), 35f).getStatus());
        // Median  35
        // Percentile 80 = 35
        // ERROR UP = 35 + 3 * diff(35 - 35) = 35
        
        Assert.assertEquals(AnalysisResult.Status.ERROR,  analysis.process(new Date(9000), 36f).getStatus());
    }
    
    @Test
    public void analysisErrorLow() throws Exception{
        PercentileAnalysis analysis = new PercentileAnalysis();
        
        Properties properties = new Properties();
        properties.put(PercentileAnalysis.PERIOD_PARAM, "3m");
        properties.put(PercentileAnalysis.WARN_RATIO_PARAM, "2");
        properties.put(PercentileAnalysis.WARN_PERCENTILE_PARAM, "60");
        properties.put(PercentileAnalysis.ERROR_RATIO_PARAM, "3");
        properties.put(PercentileAnalysis.ERROR_PERCENTILE_PARAM, "80");
        properties.put(PercentileAnalysis.ERROR_UPPERBOUND_PARAM, Boolean.FALSE.toString());
        properties.put(PercentileAnalysis.WARNING_UPPERBOUND_PARAM, Boolean.FALSE.toString());
        properties.put(PercentileAnalysis.WARNING_LOWERBOUND_PARAM, Boolean.FALSE.toString());
        properties.put(PercentileAnalysis.ERROR_LOWERBOUND_PARAM, Boolean.TRUE.toString());
        analysis.config(properties);
        
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(1000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(2000), 40f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(3000), 35f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.EXCEPTION,analysis.process(new Date(4000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(5000), 35f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(6000), 35f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(7000), 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(8000), 35f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(9000), 35f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.OK,       analysis.process(new Date(10000), 35f).getStatus());
        // Median  35
        // Percentile 20 = 30
        // ERROR LOW = 30 - 3 * diff(35 - 30) = 15
        
        Assert.assertEquals(AnalysisResult.Status.ERROR,  analysis.process(new Date(9000), 14f).getStatus());
    }
    
}
