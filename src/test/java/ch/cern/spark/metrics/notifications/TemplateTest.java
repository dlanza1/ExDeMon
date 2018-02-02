package ch.cern.spark.metrics.notifications;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;

import org.junit.Test;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.notifications.sink.types.EmailNotificationsSink;

public class TemplateTest {

    @Test
    public void templateInTag() throws ConfigurationException, AddressException, MessagingException, IOException{
        EmailNotificationsSink sink = new EmailNotificationsSink();
        
        Properties properties = new Properties();
        properties.setProperty("session.mail.smtp.host", "mmm.cern.ch");
        properties.setProperty("session.mail.smtp.auth", "true");
        properties.setProperty("username", "tapeops@cern.ch");
        properties.setProperty("password", "");
        sink.config(properties);
        sink.setSession();
        
        Notification notification = NotificationTest.DUMMY;
        Map<String, String> tags = new HashMap<>();
        tags.put("email.to", "daniel.lanza@cern.ch");
        tags.put("email.text", "Hello <tags:email.to>!");
        tags.put("cluster", "cluster1");
        notification.setTags(tags);
        notification.setNotification_timestamp(Instant.now());
        notification.setMonitor_id("MONITOR_ID");
        notification.setNotificator_id("NOTIFICATOR_ID");
        notification.setReason("In ERROR for 3 hours");
        Map<String, String> metric_attributes = new HashMap<>();
        metric_attributes.put("a", "1");
        metric_attributes.put("b", "2");
        notification.setMetric_attributes(metric_attributes);
        
        assertEquals("Hello daniel.lanza@cern.ch!", Template.apply("<tags:email.text>", notification));
    }

}
