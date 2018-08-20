package ch.cern.exdemon.monitor.trigger.action.actuator.types;

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

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.components.RegisterComponentType;
import ch.cern.exdemon.monitor.trigger.action.Action;
import ch.cern.exdemon.monitor.trigger.action.actuator.Actuator;
import ch.cern.exdemon.monitor.trigger.action.template.Template;
import ch.cern.properties.Properties;
import lombok.ToString;

@RegisterComponentType("email")
@ToString(callSuper=false)
public class EmailActuator extends Actuator {

    private static final long serialVersionUID = 7529886359533539703L;
    
    private final static Logger LOG = LogManager.getLogger(EmailActuator.class);
    
    private Properties sessionPoperties;
    
    private String username;
    private String password;

    private transient Session session;

    private String toProp;
    private String subjectProp;
    private String textProp;

    @Override
	public ConfigurationResult config(Properties properties) {
		ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
		
		sessionPoperties = properties.getSubset("session");
		
		username = properties.getProperty("username");
		if(username == null)
		    confResult.withMustBeConfigured("username");
		
		password = properties.getProperty("password");
		
		toProp = properties.getProperty("to", "<tags:email.to>");
		subjectProp = properties.getProperty("subject", "<tags:email.subject>");
		textProp = properties.getProperty("text", "<tags:email.text>");
		
		return confResult;
	}

    @Override
    protected void run(Action action) throws Exception{
        setSession();

        MimeMessage message = toMimeMessage(action);
                    
        if(message != null)
            Transport.send(message);
    }

    public MimeMessage toMimeMessage(Action action) throws AddressException, MessagingException {
        MimeMessage message = new MimeMessage(session);
        message.setFrom(new InternetAddress(username));
        
        if(toProp == null) {
            LOG.error("Email not sent becuase email.to is empty: " + action);
            return null;
        }
        for(String email_to: Template.apply(toProp, action).split(","))
            message.addRecipient(Message.RecipientType.TO, new InternetAddress(email_to.trim()));

        if(subjectProp != null && !subjectProp.equals("null"))
            message.setSubject(Template.apply(subjectProp, action));
        else
            message.setSubject(Template.apply("ExDeMon: action from monitor <moniotr_id> (<trigger_id>)", action));
        
        String content = Template.apply(textProp, action);
        
        if(!content.equals("null")) {
            content = toHtml(content);
        
            message.setContent(content, "text/html; charset=utf-8");
        }else {
            String textTemplate = "Monitor ID: <monitor_id>";
            textTemplate += "\n\nTrigger ID: <trigger_id>";
            textTemplate += "\n\nMetric attributes: <attributes:.*>";
            textTemplate += "\n\nAt: <datetime>";
            textTemplate += "\n\nReason: <reason>";
            textTemplate += "\n\nTags: <tags>";
            
            message.setContent(toHtml(Template.apply(textTemplate, action)), "text/html; charset=utf-8");
        }
        
        return message;  
    }

    private String toHtml(String text) {
        return text.replace("\n", "<br />");
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
