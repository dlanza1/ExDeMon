package ch.cern.spark.metrics.notificator.types;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import java.text.SimpleDateFormat;

import org.junit.Test;

import ch.cern.spark.Properties;
import ch.cern.spark.metrics.results.AnalysisResult.Status;

public class ConstantNotificatorTest {
    
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    public void raise() throws Exception{
        
        ConstantNotificator notificator = new ConstantNotificator();
        Properties properties = new Properties();
        properties.setProperty("period", "10m");
        properties.setProperty("statuses", "ERROR");
        notificator.config(properties);
        
        assertNull(notificator.process(Status.OK,      format.parse("2017-09-19 12:56:00")));
        assertNull(notificator.process(Status.OK,      format.parse("2017-09-19 12:57:00")));
        assertNull(notificator.process(Status.OK,      format.parse("2017-09-19 12:58:00")));
        assertNull(notificator.process(Status.OK,      format.parse("2017-09-19 12:59:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:00:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:01:00")));
        assertNull(notificator.process(Status.OK,      format.parse("2017-09-19 13:02:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:03:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:04:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:05:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:06:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:07:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:08:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:09:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:10:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:11:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:12:00")));
        assertNotNull(notificator.process(Status.ERROR,format.parse("2017-09-19 13:13:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:14:00")));
    }

    @Test
    public void severalStatuses() throws Exception{
        
        ConstantNotificator notificator = new ConstantNotificator();
        Properties properties = new Properties();
        properties.setProperty("period", "10m");
        properties.setProperty("statuses", "ERROR,WARNING");
        notificator.config(properties);
        
        assertNull(notificator.process(Status.OK,      format.parse("2017-09-19 12:56:00")));
        assertNull(notificator.process(Status.OK,      format.parse("2017-09-19 12:57:00")));
        assertNull(notificator.process(Status.OK,      format.parse("2017-09-19 12:58:00")));
        assertNull(notificator.process(Status.OK,      format.parse("2017-09-19 12:59:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:00:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:01:00")));
        assertNull(notificator.process(Status.OK,      format.parse("2017-09-19 13:02:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:03:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:04:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:05:00")));
        assertNull(notificator.process(Status.WARNING, format.parse("2017-09-19 13:06:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:07:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:08:00")));
        assertNull(notificator.process(Status.WARNING, format.parse("2017-09-19 13:09:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:10:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:11:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:12:00")));
        assertNotNull(notificator.process(Status.ERROR,format.parse("2017-09-19 13:13:00")));
        assertNull(notificator.process(Status.ERROR,   format.parse("2017-09-19 13:14:00")));
    }
    
}
