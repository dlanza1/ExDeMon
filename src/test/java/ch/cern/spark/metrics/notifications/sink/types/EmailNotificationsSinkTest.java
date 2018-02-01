package ch.cern.spark.metrics.notifications.sink.types;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;
import javax.mail.internet.MimeMessage;

import org.junit.Test;
import org.simplejavamail.converter.internal.mimemessage.MimeMessageParser;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.notifications.Notification;

public class EmailNotificationsSinkTest {
    
    @Test
    public void send() throws ConfigurationException, AddressException, MessagingException{
        EmailNotificationsSink sink = new EmailNotificationsSink();
        
        Properties properties = new Properties();
        properties.setProperty("session.mail.smtp.host", "mmm.cern.ch");
        properties.setProperty("session.mail.smtp.auth", "true");
        properties.setProperty("username", "tapeops@cern.ch");
        properties.setProperty("password", "");
        sink.config(properties);
        sink.setSession();
        
        Notification notification = new Notification();
        Map<String, String> tags = new HashMap<>();
        tags.put("email.to", "daniel.lanza@cern.ch");
        tags.put("cluster", "cluster1");
        notification.setTags(tags);
        notification.setNotification_timestamp(Instant.now());
        notification.setMonitor_id("MONITOR_ID");
        notification.setNotificator_id("NOTIFICAOTR_ID");
        notification.setReason("In ERROR for 3 hours");
        Map<String, String> metric_attributes = new HashMap<>();
        metric_attributes.put("a", "1");
        metric_attributes.put("b", "2");
        notification.setMetric_attributes(metric_attributes);
        
        //sink.sendEmail(notification);
    }
    
    @Test
    public void toMimeMessage() throws ConfigurationException, AddressException, MessagingException, IOException{
        EmailNotificationsSink sink = new EmailNotificationsSink();
        
        Properties properties = new Properties();
        properties.setProperty("session.mail.smtp.host", "mmm.cern.ch");
        properties.setProperty("session.mail.smtp.auth", "true");
        properties.setProperty("username", "tapeops@cern.ch");
        properties.setProperty("password", "");
        sink.config(properties);
        sink.setSession();
        
        Notification notification = new Notification();
        Map<String, String> tags = new HashMap<>();
        tags.put("email.to", "daniel.lanza@cern.ch");
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
        
        MimeMessage message = sink.toMimeMessage(notification);
        
        MimeMessageParser parser = new MimeMessageParser(message);
        parser.parse();
        
        assertEquals("Monitor ID: MONITOR_ID\n" + 
                        "\n" + 
                        "Notificator ID: NOTIFICATOR_ID\n" + 
                        "\n" + 
                        "Metric attributes: \n" + 
                        "\ta = 1\n" + 
                        "\tb = 2\n" + 
                        "\n" + 
                        "At: " + notification.getNotification_timestamp() + "\n" + 
                        "\n" + 
                        "Reason: In ERROR for 3 hours\n" + 
                        "\n" + 
                        "Tags: \n" + 
                        "\temail.to = daniel.lanza@cern.ch\n" + 
                        "\tcluster = cluster1", 
                parser.getPlainContent());
    }

}
