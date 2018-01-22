package ch.cern.spark.metrics.notifications.sink;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.streaming.api.java.JavaDStream;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.spark.metrics.Sink;
import ch.cern.spark.metrics.notifications.Notification;

@ComponentType(Type.NOTIFICATIONS_SINK)
public abstract class NotificationsSink extends Component implements Sink<Notification>{

    private static final long serialVersionUID = 8984201586179047078L;
    
    private String id;

    public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	@Override
	public final void sink(JavaDStream<Notification> notifications) {
		notify(notifications.filter(notif -> 
							notif.getSink_ids().contains(id)
							|| notif.getSink_ids().contains("ALL")));
	}

	protected abstract void notify(JavaDStream<Notification> notifications);
	
	public static String template(String template, Notification notification) {
	    if(template == null)
	        return null;
	    
        String text = template;
        
        text = text.replaceAll("<monitor_id>", notification.getMonitor_id());
        
        text = text.replaceAll("<notificator_id>", notification.getNotificator_id());
        
        Map<String, String> attributes = notification.getMetric_attributes() != null ? notification.getMetric_attributes() : new HashMap<>();
         
        String metric_attributes = "";
        for(Map.Entry<String, String> att: attributes.entrySet())
            metric_attributes += "\n\t" + att.getKey() + " = " + att.getValue();
        text = text.replaceAll("<metric_attributes>", metric_attributes);
        
        Matcher attMatcher = Pattern.compile("\\<metric_attributes:([^>]+)\\>").matcher(text);        
        while (attMatcher.find()) {
            for (int j = 1; j <= attMatcher.groupCount(); j++) {
                String key = attMatcher.group(j);
                
                String value = attributes.get(key);
                
                text = text.replaceAll("<metric_attributes:"+key+">", value != null ? value : "null");
                
                j++;
            }
        }
        
        text = text.replaceAll("<datetime>", String.valueOf(notification.getNotification_timestamp()));
        
        text = text.replaceAll("<reason>", notification.getReason());
        
        Map<String, String> tags = notification.getTags() != null ? notification.getTags() : new HashMap<>();
         
        String tags_attributes = "";
        for(Map.Entry<String, String> tag: tags.entrySet())
            tags_attributes += "\n\t" + tag.getKey() + " = " + tag.getValue();
        text = text.replaceAll("<tags>", tags_attributes);
        
        Matcher tagsMatcher = Pattern.compile("\\<tags:([^>]+)\\>").matcher(text);        
        while (tagsMatcher.find()) {
            for (int j = 1; j <= tagsMatcher.groupCount(); j++) {
                String key = tagsMatcher.group(j);
                
                String value = tags.get(key);
                
                text = text.replaceAll("<tags:"+key+">", value != null ? value : "null");
                
                j++;
            }
        }

        return text;
    }
	
}
