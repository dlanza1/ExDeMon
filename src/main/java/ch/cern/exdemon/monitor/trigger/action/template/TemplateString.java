package ch.cern.exdemon.monitor.trigger.action.template;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.util.matching.Regex;

public class TemplateString {

    private String template;

    public TemplateString(String template) {
        this.template = template;
    }

    @Override
    public String toString() {
        return template;
    }

    public void replace(String key, Object value) {
        key = "<".concat(key).concat(">");
        
        template = template.replaceAll(Regex.quote(key), Matcher.quoteReplacement(String.valueOf(value)));
    }

    public void replaceKeys(String mainKey, Map<String, ?> attributes) {
        replaceKeys(mainKey, new ValueSupplier() {
            @Override
            public Object get(String key) {
                return attributes != null ? attributes.get(key) : null;
            }
        });
    }
    
    public void replaceKeys(String mainKey, ValueSupplier valueSupplier) {
        Matcher matcher = Pattern.compile("\\<"+mainKey+":([^>]+)\\>").matcher(template);        
        
        while (matcher.find()) {
            String key = matcher.group(1);
            
            Object value = valueSupplier != null ? valueSupplier.get(key) : null;
            
            replace(mainKey + ":" + key, value);
        }
    }

    public void replaceContainer(String key, ValueSupplier valueSupplier) {
        int startKey = template.indexOf("<"+key+">");
        
        while (startKey > -1){
            int endKey = template.indexOf("</"+key+">", startKey);
            
            String preText = template.substring(0, startKey);
            String subTemplate = template.substring(startKey + 13, endKey);
            String postText = template.substring(endKey + 14);
        
            Object newValue = valueSupplier.get(subTemplate);
            
            template = preText + newValue + postText;

            startKey = template.indexOf("<agg_metrics>", endKey);
        }
        
    }
    
    protected TemplateString clone() {
        return new TemplateString(template);
    }
    
}
