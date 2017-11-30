package ch.cern.spark.metrics.notificator.types;

import static ch.cern.test.Utils.assertNotPresent;
import static ch.cern.test.Utils.assertPresent;

import java.time.Duration;
import java.time.Instant;

import org.junit.Test;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.utils.TimeUtils;

public class StatusesNotificatorTest {
    
	@Test
    public void raise() throws Exception{
        
		StatusesNotificator notificator = new StatusesNotificator();
        Properties properties = new Properties();
        properties.setProperty("statuses", "ERROR");
        notificator.config(properties);
        
        assertNotPresent(notificator.process(Status.OK,      TimeUtils.toInstant("2017-09-19 12:56:00")));
        assertNotPresent(notificator.process(Status.OK,      TimeUtils.toInstant("2017-09-19 12:57:00")));
        assertNotPresent(notificator.process(Status.OK,      TimeUtils.toInstant("2017-09-19 12:58:00")));
        assertNotPresent(notificator.process(Status.OK,      TimeUtils.toInstant("2017-09-19 12:59:00")));
        assertPresent(   notificator.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:00:00")));
        assertPresent(   notificator.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:01:00")));
        assertNotPresent(notificator.process(Status.OK,      TimeUtils.toInstant("2017-09-19 13:02:00")));
        assertPresent(   notificator.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:03:00")));
        assertPresent(   notificator.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:13:00")));
        assertPresent(   notificator.process(Status.ERROR,   TimeUtils.toInstant("2017-09-19 13:14:00")));
    }
	
	@Test
    public void raiseAfterRaiseWithSilent() throws Exception{
		StatusesNotificator notificator = new StatusesNotificator();
        Properties properties = new Properties();
        properties.setProperty("silent.period", "3m");
        properties.setProperty("statuses", "ERROR");
        notificator.config(properties);
        
        Instant now = Instant.now();
        
        assertPresent(   notificator.process(Status.ERROR, now));

        assertPresent(   notificator.process(Status.ERROR, now.plus(Duration.ofMinutes(9))));
        assertNotPresent(notificator.process(Status.ERROR, now.plus(Duration.ofMinutes(10))));
        assertPresent(   notificator.process(Status.ERROR, now.plus(Duration.ofMinutes(13))));
        
        assertPresent(   notificator.process(Status.ERROR, now.plus(Duration.ofMinutes(22))));
        assertNotPresent(notificator.process(Status.ERROR, now.plus(Duration.ofMinutes(23))));
    }

	@Test
    public void severalStatuses() throws Exception{
        
		StatusesNotificator notificator = new StatusesNotificator();
        Properties properties = new Properties();
        properties.setProperty("statuses", "error WARNING");
        notificator.config(properties);
        
        assertNotPresent(notificator.process(Status.OK,        TimeUtils.toInstant("2017-09-19 12:56:00")));
        assertPresent(   notificator.process(Status.ERROR,     TimeUtils.toInstant("2017-09-19 13:00:00")));
        assertPresent(   notificator.process(Status.WARNING,   TimeUtils.toInstant("2017-09-19 13:06:00")));
        assertNotPresent(notificator.process(Status.EXCEPTION, TimeUtils.toInstant("2017-09-19 12:56:00")));
    }
    
}
