package ch.cern.exdemon.monitor.analysis.types;

import org.junit.Assert;
import org.junit.Test;

import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.properties.Properties;

public class FixedThresholdAnalysisTest {

    @Test
    public void parse() throws Exception{
        FixedThresholdAnalysis analysis = new FixedThresholdAnalysis();
        
        Properties properties = new Properties();

        properties.put("error.upperbound", "110");
        properties.put("warn.upperbound", "100");
        properties.put("warn.lowerbound", "50");
        properties.put("error.lowerbound", "40");
        
        analysis.config(properties);
        
        Assert.assertEquals(110, analysis.getErrorUpperBound(), 0);
        Assert.assertEquals(100, analysis.getWarningUpperBound(), 0);
        Assert.assertEquals(50, analysis.getWarningLowerBound(), 0);
        Assert.assertEquals(40, analysis.getErrorLowerBound(), 0);
    }
    
    @Test
    public void analyzeMetriWithAllThresholdsc() throws Exception{
        FixedThresholdAnalysis analysis = new FixedThresholdAnalysis();
        
        Properties properties = new Properties();

        properties.put("error.upperbound", "110");
        properties.put("warn.upperbound", "100");
        properties.put("warn.lowerbound", "-50");
        properties.put("error.lowerbound", "-70");
        
        analysis.config(properties);
        
        Assert.assertEquals(AnalysisResult.Status.OK, analysis.process(null, 30f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR, analysis.process(null, 200f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.WARNING, analysis.process(null, 105f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.WARNING, analysis.process(null, -50f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.ERROR, analysis.process(null, -200f).getStatus());
    }
    
    @Test
    public void analyzeMetriWithoutErrorThresholdsc() throws Exception{
        FixedThresholdAnalysis analysis = new FixedThresholdAnalysis();
        
        Properties properties = new Properties();

        properties.put("warn.upperbound", "100");
        properties.put("warn.lowerbound", "50");
        
        analysis.config(properties);
        
        Assert.assertEquals(AnalysisResult.Status.OK, analysis.process(null, 75f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.WARNING, analysis.process(null, 100f).getStatus());
        Assert.assertEquals(AnalysisResult.Status.WARNING, analysis.process(null, 45f).getStatus());
    }

    @Test
    public void analyzeMetriWithoutThresholdsc() throws Exception{
        FixedThresholdAnalysis monitor = new FixedThresholdAnalysis();
        
        Properties properties = new Properties();    
        monitor.config(properties);
        
        Assert.assertEquals(AnalysisResult.Status.OK, monitor.process(null, 75f).getStatus());
    }
    
}
