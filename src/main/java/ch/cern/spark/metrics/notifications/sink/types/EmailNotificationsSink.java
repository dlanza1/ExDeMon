package ch.cern.spark.metrics.notifications.sink.types;

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
import ch.cern.spark.metrics.notifications.Template;
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
		
		toProp = properties.getProperty("to", "<tags:email.to>");
		subjectProp = properties.getProperty("subject", "<tags:email.subject>");
		textProp = properties.getProperty("text", "<tags:email.text>");
	}

    @Override
    protected void notify(JavaDStream<Notification> notifications) {
        setSession();
        
        notifications.foreachRDD(rdd -> rdd.foreach(notification -> {
                    MimeMessage message = toMimeMessage(notification);
                    
                    if(message != null)
                        Transport.send(message);
                }));
    }

    public MimeMessage toMimeMessage(Notification notification) throws AddressException, MessagingException {
        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress(username));
        
        if(toProp == null) {
            LOG.error("Email not sent becuase email.to is empty: " + notification);
            return null;
        }
        for(String email_to: Template.apply(toProp, notification).split(","))
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(email_to.trim()));
        
        if(subjectProp != null)
            message.setSubject(Template.apply(subjectProp, notification));
        else
            message.setSubject(Template.apply("ExDeMon: notification from monitor <moniotr_id> (<notificator_id>)", notification));
        
        textProp = Template.apply(textProp, notification);
        if(!textProp.equals("null"))
            message.setText(textProp);
        else {
            String textTemplate = "Monitor ID: <monitor_id>";
            textTemplate += "\n\nNotificator ID: <notificator_id>";
            textTemplate += "\n\nMetric attributes: <metric_attributes>";
            textTemplate += "\n\nAt: <datetime>";
            textTemplate += "\n\nReason: <reason>";
            textTemplate += "\n\nTags: <tags>";
            
            message.setText(Template.apply(textTemplate, notification));
        }
        
        return message;  
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
