package ch.cern.spark.metrics.notificator.types;

import static ch.cern.test.Utils.assertNotPresent;
import static ch.cern.test.Utils.assertPresent;

import java.time.Duration;
import java.time.Instant;

import org.junit.Test;

import ch.cern.properties.Properties;
import ch.cern.spark.metrics.results.AnalysisResult.Status;
import ch.cern.utils.TimeUtils;

public class ConstantNotificatorTest {

    @Test
    public void raise() throws Exception {

        ConstantNotificator notificator = new ConstantNotificator();
        Properties properties = new Properties();
        properties.setProperty("period", "10m");
        properties.setProperty("statuses", "ERROR");
        notificator.config(properties);

        assertNotPresent(notificator.process(Status.OK,     TimeUtils.toInstant("2017-09-19 12:56:00")));
        assertNotPresent(notificator.process(Status.OK,     TimeUtils.toInstant("2017-09-19 12:57:00")));
        assertNotPresent(notificator.process(Status.OK,     TimeUtils.toInstant("2017-09-19 12:58:00")));
        assertNotPresent(notificator.process(Status.OK,     TimeUtils.toInstant("2017-09-19 12:59:00")));
        assertNotPresent(notificator.process(Status.ERROR,  TimeUtils.toInstant("2017-09-19 13:00:00")));
        assertNotPresent(notificator.process(Status.ERROR,  TimeUtils.toInstant("2017-09-19 13:01:00")));
        assertNotPresent(notificator.process(Status.OK,     TimeUtils.toInstant("2017-09-19 13:02:00")));
        assertNotPresent(notificator.process(Status.ERROR,  TimeUtils.toInstant("2017-09-19 13:03:00")));

        assertPresent(notificator.process(Status.ERROR,     TimeUtils.toInstant("2017-09-19 13:13:00")));
        assertNotPresent(notificator.process(Status.ERROR,  TimeUtils.toInstant("2017-09-19 13:14:00")));
    }

    @Test
    public void raiseTimes() throws Exception {

        ConstantNotificator notificator = new ConstantNotificator();
        Properties properties = new Properties();
        properties.setProperty("max-times", "3");
        properties.setProperty("statuses", "ERROR");
        notificator.config(properties);

        assertNotPresent(notificator.process(Status.OK,     Instant.now()));
        assertNotPresent(notificator.process(Status.OK,     Instant.now()));
        assertNotPresent(notificator.process(Status.OK,     Instant.now()));
        assertNotPresent(notificator.process(Status.OK,     Instant.now()));
        assertNotPresent(notificator.process(Status.ERROR,  Instant.now()));
        assertNotPresent(notificator.process(Status.ERROR,  Instant.now()));
        assertNotPresent(notificator.process(Status.OK,     Instant.now()));
        assertNotPresent(notificator.process(Status.ERROR,  Instant.now()));
        assertNotPresent(notificator.process(Status.ERROR,  Instant.now()));
        assertPresent(notificator.process(Status.ERROR,     Instant.now()));
    }
    
    @Test
    public void raisePeriodAndTimes() throws Exception {

        ConstantNotificator notificator = new ConstantNotificator();
        Properties properties = new Properties();
        properties.setProperty("period", "10m");
        properties.setProperty("max-times", "4");
        properties.setProperty("statuses", "ERROR");
        notificator.config(properties);

        assertNotPresent(notificator.process(Status.OK,     TimeUtils.toInstant("2017-09-19 12:56:00")));
        assertNotPresent(notificator.process(Status.OK,     TimeUtils.toInstant("2017-09-19 12:57:00")));
        assertNotPresent(notificator.process(Status.ERROR,  TimeUtils.toInstant("2017-09-19 12:58:00")));
        assertNotPresent(notificator.process(Status.ERROR,  TimeUtils.toInstant("2017-09-19 12:59:00")));
        assertNotPresent(notificator.process(Status.ERROR,  TimeUtils.toInstant("2017-09-19 13:00:00")));
        assertPresent(notificator.process(Status.ERROR,     TimeUtils.toInstant("2017-09-19 13:01:00")));
        assertNotPresent(notificator.process(Status.OK,     TimeUtils.toInstant("2017-09-19 13:02:00")));
        assertNotPresent(notificator.process(Status.ERROR,  TimeUtils.toInstant("2017-09-19 13:03:00")));

        assertPresent(notificator.process(Status.ERROR,     TimeUtils.toInstant("2017-09-19 13:13:00")));
        assertNotPresent(notificator.process(Status.ERROR,  TimeUtils.toInstant("2017-09-19 13:14:00")));
    }

    @Test
    public void raiseAfterRaiseWithSilent() throws Exception {
        ConstantNotificator notificator = new ConstantNotificator();
        Properties properties = new Properties();
        properties.setProperty("period", "10m");
        properties.setProperty("silent.period", "3m");
        properties.setProperty("statuses", "ERROR");
        notificator.config(properties);

        Instant now = Instant.now();

        assertNotPresent(notificator.process(Status.ERROR, now));

        assertNotPresent(notificator.process(Status.ERROR, now.plus(Duration.ofMinutes(9))));
        assertPresent(notificator.process(Status.ERROR, now.plus(Duration.ofMinutes(10))));
        assertNotPresent(notificator.process(Status.ERROR, now.plus(Duration.ofMinutes(13))));

        assertNotPresent(notificator.process(Status.ERROR, now.plus(Duration.ofMinutes(22))));
        assertPresent(notificator.process(Status.ERROR, now.plus(Duration.ofMinutes(23))));
    }

    @Test
    public void severalStatuses() throws Exception {

        ConstantNotificator notificator = new ConstantNotificator();
        Properties properties = new Properties();
        properties.setProperty("period", "10m");
        properties.setProperty("statuses", "error WARNING");
        notificator.config(properties);

        assertNotPresent(notificator.process(Status.OK, TimeUtils.toInstant("2017-09-19 12:56:00")));
        assertNotPresent(notificator.process(Status.OK, TimeUtils.toInstant("2017-09-19 12:57:00")));
        assertNotPresent(notificator.process(Status.OK, TimeUtils.toInstant("2017-09-19 12:58:00")));
        assertNotPresent(notificator.process(Status.OK, TimeUtils.toInstant("2017-09-19 12:59:00")));
        assertNotPresent(notificator.process(Status.ERROR, TimeUtils.toInstant("2017-09-19 13:00:00")));
        assertNotPresent(notificator.process(Status.ERROR, TimeUtils.toInstant("2017-09-19 13:01:00")));
        assertNotPresent(notificator.process(Status.OK, TimeUtils.toInstant("2017-09-19 13:02:00")));
        assertNotPresent(notificator.process(Status.ERROR, TimeUtils.toInstant("2017-09-19 13:03:00")));
        assertNotPresent(notificator.process(Status.ERROR, TimeUtils.toInstant("2017-09-19 13:04:00")));
        assertNotPresent(notificator.process(Status.ERROR, TimeUtils.toInstant("2017-09-19 13:05:00")));
        assertNotPresent(notificator.process(Status.WARNING, TimeUtils.toInstant("2017-09-19 13:06:00")));
        assertNotPresent(notificator.process(Status.ERROR, TimeUtils.toInstant("2017-09-19 13:07:00")));
        assertNotPresent(notificator.process(Status.ERROR, TimeUtils.toInstant("2017-09-19 13:08:00")));
        assertNotPresent(notificator.process(Status.WARNING, TimeUtils.toInstant("2017-09-19 13:09:00")));
        assertNotPresent(notificator.process(Status.ERROR, TimeUtils.toInstant("2017-09-19 13:10:00")));
        assertNotPresent(notificator.process(Status.ERROR, TimeUtils.toInstant("2017-09-19 13:11:00")));
        assertNotPresent(notificator.process(Status.ERROR, TimeUtils.toInstant("2017-09-19 13:12:00")));
        assertPresent(notificator.process(Status.ERROR, TimeUtils.toInstant("2017-09-19 13:13:00")));
        assertNotPresent(notificator.process(Status.ERROR, TimeUtils.toInstant("2017-09-19 13:14:00")));
    }

}
