package ch.cern.exdemon.monitor.trigger.action.silence;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.junit.Test;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.monitor.analysis.results.AnalysisResult;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.properties.Properties;

public class SilenceTest {

    @Test
    public void activeWithoutFromAndTo() {
        Silence silence = new Silence();
        silence.config(new Properties());
        
        assertTrue(silence.isActiveAt(Instant.now()));
    }

    @Test
    public void wrongFormatConfig() {
        Silence silence = new Silence();
        Properties properties = new Properties();
        properties.setProperty("from", "2018-02-10WRONG10:12:12");
        
        ConfigurationResult confResult = silence.config(properties);
        
        assertEquals("could not be parsed, expect format \"yyyy-MM-dd HH:mm:ss\" (e.g: 2016-02-16 11:00:02)", confResult.getErrors().get(0).getMessage());
    }

    @Test
    public void activeDuringDuration() {
        Silence silence = new Silence();
        Properties properties = new Properties();
        properties.setProperty("from", "2018-02-10 10:12:12");
        properties.setProperty("duration", "1w");
        silence.config(properties);
        
        assertTrue(silence.isActiveAt(Instant.parse("2018-02-12T10:15:30.00Z")));
        assertFalse(silence.isActiveAt(Instant.parse("2018-12-20T10:15:30.00Z")));
    }
    
    @Test
    public void activeWithFrom() {
        Silence silence = new Silence();
        Properties properties = new Properties();
        properties.setProperty("from", "2018-02-10 10:12:12");
        silence.config(properties );
        
        assertFalse(silence.isActiveAt(Instant.parse("2007-12-03T10:15:30.00Z")));
        assertTrue(silence.isActiveAt(Instant.parse("2019-12-03T10:15:30.00Z")));
    }
    
    @Test
    public void activeWithTo() {
        Silence silence = new Silence();
        Properties properties = new Properties();
        properties.setProperty("to", "2018-02-10 10:12:12");
        silence.config(properties );
        
        assertTrue(silence.isActiveAt(Instant.parse("2007-12-03T10:15:30.00Z")));
        assertFalse(silence.isActiveAt(Instant.parse("2019-12-03T10:15:30.00Z")));
    }
    
    @Test
    public void activeWithFromAndTo() {
        Silence silence = new Silence();
        Properties properties = new Properties();
        properties.setProperty("from", "2017-02-10 10:12:12");
        properties.setProperty("to", "2018-02-10 10:12:12");
        silence.config(properties );
        
        assertFalse(silence.isActiveAt(Instant.parse("2007-12-03T10:15:30.00Z")));
        assertTrue(silence.isActiveAt(Instant.parse("2017-12-03T10:15:30.00Z")));
        assertFalse(silence.isActiveAt(Instant.parse("2019-12-03T10:15:30.00Z")));
    }
    
    @Test
    public void silentAction() {
        Silence silence = new Silence();
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.att1", "val1");
        silence.config(properties);
        
        Map<String, String> metric_attributes = new HashMap<>();
        metric_attributes.put("att1", "val1");
        Action action = new Action("", "", metric_attributes, "", new HashSet<>(), new HashMap<>(), new AnalysisResult());
        
        assertFalse(silence.filter(action));
    }
    
    @Test
    public void allowAction() {
        Silence silence = new Silence();
        Properties properties = new Properties();
        properties.setProperty("filter.attribute.att1", "val1");
        silence.config(properties);
        
        Map<String, String> metric_attributes = new HashMap<>();
        metric_attributes.put("att1", "no_valid_val");
        Action action = new Action("", "", metric_attributes, "", new HashSet<>(), new HashMap<>(), new AnalysisResult());
        
        assertTrue(silence.filter(action));
    }
    
}
