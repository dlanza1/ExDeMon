package ch.cern.spark.metrics.notifications;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import ch.cern.spark.metrics.results.AnalysisResult;
import lombok.NonNull;

public class Template {
    
    public static String apply(String template, @NonNull Notification notification) {
        if(template == null)
            return null;
        
        String text = template;
        
        text = text.replaceAll("<monitor_id>", String.valueOf(notification.getMonitor_id()));
        
        text = text.replaceAll("<notificator_id>", String.valueOf(notification.getNotificator_id()));
        
        Map<String, String> attributes = notification.getMetric_attributes() != null ? notification.getMetric_attributes() : new HashMap<>();
         
        String metric_attributes = "";
        for(Map.Entry<String, String> att: attributes.entrySet())
            metric_attributes += "\n\t" + att.getKey() + " = " + att.getValue();
        if(attributes.size() != 0)
            text = text.replaceAll("<metric_attributes>", metric_attributes);
        else
            text = text.replaceAll("<metric_attributes>", "(empty)");
        
        Matcher attMatcher = Pattern.compile("\\<metric_attributes:([^>]+)\\>").matcher(text);        
        while (attMatcher.find()) {
            for (int j = 1; j <= attMatcher.groupCount(); j++) {
                String key = attMatcher.group(j);
                
                String value = attributes.get(key);
                
                text = text.replaceAll("<metric_attributes:"+key+">", String.valueOf(value));
                
                j++;
            }
        }
        
        text = text.replaceAll("<datetime>", String.valueOf(notification.getNotification_timestamp()));
        
        text = text.replaceAll("<reason>", String.valueOf(notification.getReason()));
        
        Map<String, String> tags = notification.getTags() != null ? notification.getTags() : new HashMap<>();
         
        String tags_attributes = "";
        for(Map.Entry<String, String> tag: tags.entrySet())
            tags_attributes += "\n\t" + tag.getKey() + " = " + tag.getValue();
        if(tags.size() != 0)
            text = text.replaceAll("<tags>", tags_attributes);
        else
            text = text.replaceAll("<tags>", "(empty)");
        
        Matcher tagsMatcher = Pattern.compile("\\<tags:([^>]+)\\>").matcher(text);        
        while (tagsMatcher.find()) {
            for (int j = 1; j <= tagsMatcher.groupCount(); j++) {
                String key = tagsMatcher.group(j);
                
                String value = apply(tags.get(key), notification);
                
                text = text.replaceAll("<tags:"+key+">", String.valueOf(value));
                
                j++;
            }
        }

        AnalysisResult triggeringResult = notification.getTriggeringResult();
        
        triggeringResult.getAnalyzed_metric().getValue();
        text = text.replaceAll("<triggering_value>", String.valueOf(notification.getReason()));
        
        Map<String, Object> analysisParams = triggeringResult.getAnalysisParams();
        Matcher analysisParamMatcher = Pattern.compile("\\<analysis_param:([^>]+)\\>").matcher(text);        
        while (analysisParamMatcher.find()) {
            for (int j = 1; j <= analysisParamMatcher.groupCount(); j++) {
                String key = analysisParamMatcher.group(j);
                
                String value = String.valueOf(analysisParams.get(key));
                
                text = text.replaceAll("<analysis_param:"+key+">", value);
                
                j++;
            }
        }
        
        return text;
    }

}
