package ch.cern.exdemon.metrics.schema;

import java.io.Serializable;
import java.text.ParseException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.json.JSON;
import ch.cern.properties.Properties;
import ch.cern.utils.Pair;
import lombok.Getter;
import lombok.ToString;

@ToString
public class AttributeDescriptor implements Serializable{

    private static final long serialVersionUID = 4204549634311771257L;
    
    @Getter
    private String alias;

    private String key;
    private Pattern keyPattern;
    
    @Getter
    private String fixedValue;
    
    private Pattern valueRegex;

    public AttributeDescriptor(String alias) {
        this.alias = alias;
    }

    public ConfigurationResult config(Properties properties) {
        ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
        
        if(!properties.containsKey("key") && !properties.containsKey("value"))
            confResult.withMustBeConfigured("key or value");
        
        if(properties.containsKey("key") && properties.containsKey("value"))
            confResult.withWarning("key and value", "both are configured, value will be used");
        
        String configuredKey = properties.getProperty("key");
        if(configuredKey != null) {
            if(configuredKey.contains("(") && configuredKey.contains(")")) {
                try {
                    keyPattern = Pattern.compile(configuredKey);
                    
                    if(!alias.contains("+"))
                        confResult.withError(null, "must contian a \"+\" if regular expression is used");
                }catch(Exception e){
                    confResult.withError("key (pattern)", e);
                }
            }else if (alias.contains("+")) {
                confResult.withError(null, "contian a \"+\" but no regular expression with one group is used");
            }else {
                key = configuredKey;
            }
        }
        
        fixedValue = properties.getProperty("value");
        
        String configuredValueRegex = properties.getProperty("regex");
        if(configuredValueRegex != null) {
            try {
                valueRegex = Pattern.compile(configuredValueRegex);
                
                if(valueRegex.matcher("").groupCount() == 2)
                    confResult.withError("regex", "must contian one group");
            }catch(Exception e){
                confResult.withError("regex", e);
            }
        }
        
        return confResult;
    }

    public Map<String, String> extract(JSON jsonObject) throws ParseException {
        Map<String, String> atts = new HashMap<>();
        
        if(fixedValue != null) {
            atts.put(alias, fixedValue);
        }else if(key != null && key.startsWith("#")){ //TODO DEPRECATED
            atts.put(alias, key.substring(1));
        }else if(key != null){
            String value = jsonObject.getProperty(key);
            
            if(value != null)
                atts.put(alias, value);
        }else if(keyPattern != null){
            String[] keys = jsonObject.getAllKeys();
            
            for (String key : keys) {
                Matcher keyMatcher = keyPattern.matcher(key);
                
                if(keyMatcher.find()) {
                    String finalAlias = alias.replace("+", keyMatcher.group(1));
                    
                    atts.put(finalAlias, jsonObject.getProperty(key));
                }
            }
        }
        
        if(valueRegex != null)
            atts = atts.entrySet().stream()
                                  .map(entry -> {
                                          Matcher matcher = valueRegex.matcher(entry.getValue());
                                          matcher.find();
                                          
                                          return new Pair<>(entry.getKey(), matcher.group(1));
                                      })
                                  .collect(Collectors.toMap(Pair::first, Pair::second));
        
        return atts;
    }

}
