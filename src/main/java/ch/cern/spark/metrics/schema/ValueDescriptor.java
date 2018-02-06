package ch.cern.spark.metrics.schema;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.json.JSONObject;
import ch.cern.spark.metrics.value.BooleanValue;
import ch.cern.spark.metrics.value.FloatValue;
import ch.cern.spark.metrics.value.StringValue;
import ch.cern.spark.metrics.value.Value;
import lombok.Getter;
import lombok.ToString;

@ToString
public class ValueDescriptor implements Serializable{

    private static final long serialVersionUID = -1567807965329553585L;

    @Getter
    private String id;

    private String key;
    public static final String KEY_PARAM = "key";
    
    private Pattern regex;
    public static final String REGEX_PARAM = "regex";
    
    private enum Type {STRING, NUMERIC, BOOLEAN, AUTO};
    private Type type;
    public static final String TYPE_PARAM = "type";

    public ValueDescriptor(String id) {
        this.id = id;
    }
    
    public void config(Properties props) throws ConfigurationException {
        key = props.getProperty(KEY_PARAM);
        if(key == null)
            throw new ConfigurationException(KEY_PARAM + " must be configured");
        
        String regexString = props.getProperty(REGEX_PARAM );
        if(regexString != null) {
            regex = Pattern.compile(regexString);
            
            if(regex.matcher("").groupCount() != 1)
                throw new ConfigurationException("Regex expression must contain exactly 1 capture group from which value will be extracted");
        }else{
            regex = null;
        }
        
        String typeString = props.getProperty(TYPE_PARAM);
        if(typeString != null)
            type = Type.valueOf(typeString.toUpperCase());
        else
            type = Type.AUTO;
        
        props.confirmAllPropertiesUsed();
    }

    public Optional<Value> extract(JSONObject jsonObject) throws ParseException {
        JsonElement element = jsonObject.getElement(key);
        
        if (element == null || element.isJsonNull())
            return Optional.empty();
        
        if (element.isJsonPrimitive()) {
            JsonPrimitive primitive = element.getAsJsonPrimitive();
            
            if(regex != null) {
                Matcher matcher = regex.matcher(primitive.getAsString());
                
                if(matcher.find()) {
                    String valueString = matcher.group(1);
                    
                    return Optional.ofNullable(toValue(valueString));
                }else {
                    return Optional.empty();
                }
            }
            
            return Optional.ofNullable(toValue(primitive));
        }
        
        return Optional.empty();
    }

    private Value toValue(JsonPrimitive primitive) {
        if(type.equals(Type.AUTO)) {
            if (primitive.isNumber())
                return new FloatValue(primitive.getAsFloat());
            else if (primitive.isBoolean())
                return new BooleanValue(primitive.getAsBoolean());
            else
                return new StringValue(primitive.getAsString());
        }
        
        return toValue(primitive.getAsString());
    }

    private Value toValue(String valueString) {
        switch (type) {
        case STRING:
            return new StringValue(valueString);
        case NUMERIC:
            try {
                return new FloatValue(Double.valueOf(valueString));
            }catch(Exception e) {}
        case BOOLEAN:
            if(valueString.toLowerCase().equals("true"))
                return new BooleanValue(true);
            else if(valueString.toLowerCase().equals("false"))
                return new BooleanValue(false);
            else
                return null;
        case AUTO:
            try {
                return new FloatValue(Double.valueOf(valueString));
            }catch(Exception e) {
                if(valueString.toLowerCase().equals("true"))
                    return new BooleanValue(true);
                else if(valueString.toLowerCase().equals("false"))
                    return new BooleanValue(false);
                else
                    return new StringValue(valueString);
            }
        }
        
        throw new RuntimeException("an option of the enum was not covered");
    }

}
