package ch.cern.spark.metrics.notifications.sink.types;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.AddressException;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.components.RegisterComponent;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.notifications.Notification;
import ch.cern.spark.metrics.notifications.sink.NotificationsSink;

@RegisterComponent("email")
public class EmailNotificationsSink extends NotificationsSink {

    private static final long serialVersionUID = 7529886359533539703L;
    
    private final static Logger LOG = LogManager.getLogger(EmailNotificationsSink.class);
    
    private Properties sessionPoperties;
    
    private String username;
    private String password;

    private transient Session session;

    private String toProp;
    private String subjectProp;
    private String textProp;

    @Override
	public void config(Properties properties) throws ConfigurationException {
		super.config(properties);
		
		sessionPoperties = properties.getSubset("session");
		
		username = properties.getProperty("username");
		if(username == null)
		    throw new ConfigurationException("Username must be specified.");
		
		password = properties.getProperty("password");
		
		toProp = properties.getProperty("to", "%email.to");
		subjectProp = properties.getProperty("subject", "%email.subject");
		textProp = properties.getProperty("text", "%email.text");
	}

    @Override
    protected void notify(JavaDStream<Notification> notifications) {
        setSession();
        
        notifications.foreachRDD(rdd -> {rdd.foreach(notification -> sendEmail(notification));});
    }

    public void sendEmail(Notification notification) throws AddressException, MessagingException {
        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress(username));
        
        String emails_to = getValue(notification.getTags(), toProp);
        notification.getTags().remove("email.to");
        if(emails_to == null) {
            LOG.error("Email not sent becuase email.to tag is empty: " + notification);
            return;
        }
        for(String email_to: emails_to.split(","))
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(email_to.trim()));
        
        String subject = getValue(notification.getTags(), subjectProp);
        notification.getTags().remove("email.subject");
        if(subject != null)
            message.setSubject(applyTemplate(subject, notification));
        else
            message.setSubject(applyTemplate("ExDeMon: notification from monitor <moniotr_id> (<notificator_id>)", notification));
        
        String text = getValue(notification.getTags(), textProp);
        notification.getTags().remove("email.text");
        if(text != null)
            message.setText(applyTemplate(text, notification));
        else {
            String textTemplate = "Monitor ID: <monitor_id>";
            textTemplate += "\n\nNotificator ID: <notificator_id>";
            textTemplate += "\n\nMetric attributes: <metric_attributes>";
            textTemplate += "\n\nAt: <datetime>";
            textTemplate += "\n\nReason: <reason>";
            textTemplate += "\n\nTags: <tags>";
            
            message.setText(applyTemplate(textTemplate, notification));
        }
            
        Transport.send(message);  
    }

    private String applyTemplate(String template, Notification notification) {
        String text = template;
        
        text = text.replaceAll("<monitor_id>", notification.getMonitor_id());
        
        text = text.replaceAll("<notificator_id>", notification.getNotificator_id());
        
        String metric_attributes = "";
        for(Map.Entry<String, String> att: notification.getMetric_attributes().entrySet())
            metric_attributes += "\n\t" + att.getKey() + " = " + att.getValue();
        text = text.replaceAll("<metric_attributes>", metric_attributes);
        
        Matcher attMatcher = Pattern.compile("\\<metric_attributes:([^>]+)\\>").matcher(text);        
        while (attMatcher.find()) {
            for (int j = 1; j <= attMatcher.groupCount(); j++) {
                String key = attMatcher.group(j);
                
                String value = notification.getMetric_attributes().get(key);
                
                text = text.replaceAll("<metric_attributes:"+key+">", value != null ? value : "null");
                
                j++;
            }
        }
        
        text = text.replaceAll("<datetime>", notification.getNotification_timestamp().toString());
        
        text = text.replaceAll("<reason>", notification.getReason());
        
        String tags_attributes = "";
        for(Map.Entry<String, String> tag: notification.getTags().entrySet())
            tags_attributes += "\n\t" + tag.getKey() + " = " + tag.getValue();
        text = text.replaceAll("<tags>", tags_attributes);
        
        Matcher tagsMatcher = Pattern.compile("\\<tags:([^>]+)\\>").matcher(text);        
        while (tagsMatcher.find()) {
            for (int j = 1; j <= tagsMatcher.groupCount(); j++) {
                String key = tagsMatcher.group(j);
                
                String value = notification.getMetric_attributes().get(key);
                
                text = text.replaceAll("<tags:"+key+">", value != null ? value : "null");
                
                j++;
            }
        }

        return text;
    }

    private String getValue(Map<String, String> tags, String value) {
        if(value.startsWith("%"))
            return tags.get(value.substring(1));
        
        return value;
    }

    public void setSession() {
        if(session != null)
            return;
            
        Authenticator auth = null;
        if(username != null && password != null)
            auth = new javax.mail.Authenticator() {
                protected PasswordAuthentication getPasswordAuthentication() {  
                    return new PasswordAuthentication(username, password);  
                }
            };
        
        session = Session.getInstance(sessionPoperties, auth);
    }

}
