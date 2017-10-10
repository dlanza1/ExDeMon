package ch.cern.spark.metrics.notificator.types;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.text.SimpleDateFormat;
import java.time.Instant;

import org.junit.Test;

import ch.cern.spark.Properties;
import ch.cern.spark.TimeUtils;
import ch.cern.spark.metrics.results.AnalysisResult.Status;

public class PercentageNotificatorTest {
    
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    @Test
    public void raise() throws Exception{
        
        PercentageNotificator notificator = new PercentageNotificator();
        Properties properties = new Properties();
        properties.setProperty("period", "10m");
        properties.setProperty("statuses", "ERROR");
        properties.setProperty("percentage", "70");
        notificator.config(properties);
        
        assertNull(notificator.process(Status.OK,      TimeUtils.toInstant("2017-09-19 12:56:00")));
        assertNull(notificator.process(Status.OK,      TimeUtils.toInstant("2017-09-19 12:57:00")));
        assertNull(notificator.process(Status.OK,      TimeUtils.toInstant("2017-09-19 12:58:00")));
        assertNull(notificator.process(Status.OK,      TimeUtils.toInstant("2017-09-19 12:59:00")));
        assertNull(notificator.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:00:00")));
        assertNull(notificator.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:01:00")));
        assertNull(notificator.process(Status.OK,      TimeUtils.toInstant("2017-09-19 13:02:00")));
        assertNull(notificator.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:03:00")));
        assertNull(notificator.process(Status.OK,      TimeUtils.toInstant("2017-09-19 13:04:00")));
        assertNull(notificator.process(Status.OK,      TimeUtils.toInstant("2017-09-19 13:05:00")));
        assertNull(notificator.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:06:00")));
        assertNull(notificator.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:07:00")));
        assertNull(notificator.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:08:00")));
        assertNull(notificator.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:09:00")));
        assertNotNull(notificator.process(Status.ERROR,TimeUtils.toInstant("2017-09-19 13:10:00")));
        assertNull(notificator.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:11:00")));
    }
    
    @Test
    public void notCoveredByData() throws Exception{
        
        PercentageNotificator notificator = new PercentageNotificator();
        Properties properties = new Properties();
        properties.setProperty("period", "4m");
        properties.setProperty("statuses", "ERROR");
        properties.setProperty("percentage", "70");
        notificator.config(properties);
        
        assertNull(notificator.process(Status.ERROR, Instant.ofEpochSecond(10)));
        assertNull(notificator.process(Status.ERROR, Instant.ofEpochSecond(20)));
        assertNull(notificator.process(Status.ERROR, Instant.ofEpochSecond(30)));
        assertNull(notificator.process(Status.ERROR, Instant.ofEpochSecond(40))); 
    }
    
    @Test
    public void expire() throws Exception{
        
        PercentageNotificator notificator = new PercentageNotificator();
        Properties properties = new Properties();
        properties.setProperty("period", "4m");
        properties.setProperty("statuses", "ERROR");
        properties.setProperty("percentage", "70");
        notificator.config(properties);
        
        assertNull(notificator.process(Status.ERROR,   Instant.ofEpochSecond(1 * 60)));
        assertNull(notificator.process(Status.WARNING, Instant.ofEpochSecond(4 * 60)));
        assertNull(notificator.process(Status.ERROR,   Instant.ofEpochSecond(5 * 60)));
        assertNull(notificator.process(Status.ERROR,   Instant.ofEpochSecond(6 * 60))); 
    }
    
}
