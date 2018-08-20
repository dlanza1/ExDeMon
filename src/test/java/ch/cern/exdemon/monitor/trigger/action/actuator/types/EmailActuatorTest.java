package ch.cern.exdemon.monitor.trigger.action.actuator.types;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import javax.mail.MessagingException;
import javax.mail.internet.AddressException;
import javax.mail.internet.MimeMessage;

import org.junit.Test;

import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.exdemon.monitor.trigger.action.ActionTest;
import ch.cern.exdemon.monitor.trigger.action.template.Template;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;

public class EmailActuatorTest {
    
    @Test
    public void send() throws Exception{
        EmailActuator sink = new EmailActuator();
        
        Properties properties = new Properties();
        properties.setProperty("session.mail.smtp.host", "mmm.cern.ch");
        properties.setProperty("session.mail.smtp.auth", "true");
        properties.setProperty("username", "exdemon.notifications@cern.ch");
        properties.setProperty("password", "");
        sink.config(properties);
        sink.setSession();
        
        Action action = ActionTest.DUMMY;
        Map<String, String> tags = new HashMap<>();
        tags.put("email.to", "daniel.lanza@cern.ch");
        tags.put("cluster", "cluster1");
        action.setTags(tags);
        action.setCreation_timestamp(Instant.now());
        action.setMonitor_id("MONITOR_ID");
        action.setTrigger_id("NOTIFICAOTR_ID");
        action.setReason("In ERROR for 3 hours");
        Map<String, String> metric_attributes = new HashMap<>();
        metric_attributes.put("a", "1");
        metric_attributes.put("b", "2");
        action.setMetric_attributes(metric_attributes);
        
        //sink.run(action);
    }
    
    @Test
    public void toMimeMessage() throws ConfigurationException, AddressException, MessagingException, IOException{
        EmailActuator sink = new EmailActuator();
        
        Properties properties = new Properties();
        properties.setProperty("session.mail.smtp.host", "mmm.cern.ch");
        properties.setProperty("session.mail.smtp.auth", "true");
        properties.setProperty("username", "tapeops@cern.ch");
        properties.setProperty("password", "");
        sink.config(properties);
        sink.setSession();
        
        Action action = ActionTest.DUMMY;
        Map<String, String> tags = new HashMap<>();
        tags.put("email.to", "daniel.lanza@cern.ch");
        tags.put("cluster", "cluster1");
        action.setTags(tags);
        action.setCreation_timestamp(Instant.now());
        action.setMonitor_id("MONITOR_ID");
        action.setTrigger_id("TRIGGER_ID");
        action.setReason("In ERROR for 3 hours");
        Map<String, String> metric_attributes = new HashMap<>();
        metric_attributes.put("a", "1");
        metric_attributes.put("b", "2");
        action.setMetric_attributes(metric_attributes);
        
        MimeMessage message = sink.toMimeMessage(action);

        assertEquals("Monitor ID: MONITOR_ID<br />" + 
                        "<br />" + 
                        "Trigger ID: TRIGGER_ID<br />" + 
                        "<br />" + 
                        "Metric attributes: <br />" + 
                        "a = 1<br />" + 
                        "b = 2<br />" + 
                        "<br />" + 
                        "At: " + Template.dateFormatter.format(action.getCreation_timestamp()) + "<br />" + 
                        "<br />" + 
                        "Reason: In ERROR for 3 hours<br />" + 
                        "<br />" + 
                        "Tags: <br />" + 
                        "email.to = daniel.lanza@cern.ch<br />" + 
                        "cluster = cluster1", 
                        message.getContent());
    }

}
