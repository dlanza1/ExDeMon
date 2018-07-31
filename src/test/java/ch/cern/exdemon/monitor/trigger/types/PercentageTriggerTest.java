package ch.cern.exdemon.monitor.trigger.types;

import static ch.cern.test.Utils.assertNotPresent;
import static ch.cern.test.Utils.assertPresent;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.Instant;

import org.junit.Test;

import ch.cern.exdemon.monitor.analysis.results.AnalysisResult.Status;
import ch.cern.properties.Properties;
import ch.cern.utils.TimeUtils;

public class PercentageTriggerTest {
    
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Test
    public void raise() throws Exception{
        
        PercentageTrigger trigger = new PercentageTrigger();
        Properties properties = new Properties();
        properties.setProperty("period", "10m");
        properties.setProperty("statuses", "ERROR");
        properties.setProperty("percentage", "70");
        trigger.config(properties);
        
        assertNotPresent(trigger.process(Status.OK,      TimeUtils.toInstant("2017-09-19 12:56:00")));
        assertNotPresent(trigger.process(Status.OK,      TimeUtils.toInstant("2017-09-19 12:57:00")));
        assertNotPresent(trigger.process(Status.OK,      TimeUtils.toInstant("2017-09-19 12:58:00")));
        assertNotPresent(trigger.process(Status.OK,      TimeUtils.toInstant("2017-09-19 12:59:00")));
        assertNotPresent(trigger.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:00:00")));
        assertNotPresent(trigger.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:01:00")));
        assertNotPresent(trigger.process(Status.OK,      TimeUtils.toInstant("2017-09-19 13:02:00")));
        assertNotPresent(trigger.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:03:00")));
        assertNotPresent(trigger.process(Status.OK,      TimeUtils.toInstant("2017-09-19 13:04:00")));
        assertNotPresent(trigger.process(Status.OK,      TimeUtils.toInstant("2017-09-19 13:05:00")));
        assertNotPresent(trigger.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:06:00")));
        assertNotPresent(trigger.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:07:00")));
        assertNotPresent(trigger.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:08:00")));
        assertNotPresent(trigger.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:09:00")));
        assertPresent(trigger.process(Status.ERROR,		TimeUtils.toInstant("2017-09-19 13:10:00")));
        assertNotPresent(trigger.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:11:00")));
    }
    
	@Test
    public void raiseAfterRaise() throws Exception{
		PercentageTrigger trigger = new PercentageTrigger();
        Properties properties = new Properties();
        properties.setProperty("period", "10m");
        properties.setProperty("statuses", "ERROR");
        trigger.config(properties);
        
        Instant now = Instant.now();
        
        assertNotPresent(trigger.process(Status.ERROR, now));

        assertNotPresent(trigger.process(Status.ERROR, now.plus(Duration.ofMinutes(9))));
        assertPresent(   trigger.process(Status.ERROR, now.plus(Duration.ofMinutes(10))));
        assertNotPresent(trigger.process(Status.ERROR, now.plus(Duration.ofMinutes(15))));
        
        assertNotPresent(trigger.process(Status.ERROR, now.plus(Duration.ofMinutes(24))));
        assertPresent(   trigger.process(Status.ERROR, now.plus(Duration.ofMinutes(25))));
    }
    
    @Test
    public void notCoveredByData() throws Exception{
        
        PercentageTrigger trigger = new PercentageTrigger();
        Properties properties = new Properties();
        properties.setProperty("period", "4m");
        properties.setProperty("statuses", "ERROR");
        properties.setProperty("percentage", "70");
        trigger.config(properties);
        
        assertNotPresent(trigger.process(Status.ERROR, Instant.ofEpochSecond(10)));
        assertNotPresent(trigger.process(Status.ERROR, Instant.ofEpochSecond(20)));
        assertNotPresent(trigger.process(Status.ERROR, Instant.ofEpochSecond(30)));
        assertNotPresent(trigger.process(Status.ERROR, Instant.ofEpochSecond(40))); 
    }
    
    @Test
    public void expire() throws Exception{
        
        PercentageTrigger trigger = new PercentageTrigger();
        Properties properties = new Properties();
        properties.setProperty("period", "4m");
        properties.setProperty("statuses", "ERROR");
        properties.setProperty("percentage", "70");
        trigger.config(properties);
        
        assertNotPresent(trigger.process(Status.ERROR,   Instant.ofEpochSecond(1 * 60)));
        assertNotPresent(trigger.process(Status.WARNING, Instant.ofEpochSecond(4 * 60)));
        assertNotPresent(trigger.process(Status.ERROR,   Instant.ofEpochSecond(5 * 60)));
        assertNotPresent(trigger.process(Status.ERROR,   Instant.ofEpochSecond(6 * 60))); 
    }
    
}
