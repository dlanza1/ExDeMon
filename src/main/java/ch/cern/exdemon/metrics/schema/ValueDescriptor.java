package ch.cern.exdemon.metrics.schema;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.when;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.exdemon.json.JSON;
import ch.cern.exdemon.metrics.value.BooleanValue;
import ch.cern.exdemon.metrics.value.FloatValue;
import ch.cern.exdemon.metrics.value.StringValue;
import ch.cern.exdemon.metrics.value.Value;
import ch.cern.properties.Properties;
import lombok.Getter;
import lombok.ToString;

@ToString
public class ValueDescriptor implements Serializable{

    private static final long serialVersionUID = -1567807965329553585L;
    
    private static final Pattern NUMERIC_PATTERN = Pattern.compile("-?\\d+(\\.\\d+)?");

    @Getter
    private String id;

    private String key;
    public static final String KEY_PARAM = "key";
    
    private Pattern regex;
    public static final String REGEX_PARAM = "regex";
    
    public enum Type {STRING, NUMERIC, BOOLEAN, AUTO};
    private Type type;
    public static final String TYPE_PARAM = "type";

    public ValueDescriptor(String id) {
        this.id = id;
    }
    
    public ConfigurationResult config(Properties props) {
        ConfigurationResult confResult = ConfigurationResult.SUCCESSFUL();
        
        key = props.getProperty(KEY_PARAM);
        if(key == null)
            confResult.withMustBeConfigured(KEY_PARAM);
        
        String regexString = props.getProperty(REGEX_PARAM);
        if(regexString != null) {
            regex = Pattern.compile(regexString);
            
            if(regex.matcher("").groupCount() != 1)
                confResult.withError(REGEX_PARAM, "regex expression must contain exactly 1 capture group from which value will be extracted");
        }else{
            regex = null;
        }
        
        String typeString = props.getProperty(TYPE_PARAM);
        if(typeString != null)
            type = Type.valueOf(typeString.toUpperCase());
        else
            type = Type.AUTO;
        
        return confResult.merge(null, props.warningsIfNotAllPropertiesUsed());
    }

    public Optional<Value> extract(JSON jsonObject) throws ParseException {
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
                return new FloatValue(Float.parseFloat(valueString));
            }catch(Exception e) {}
        case BOOLEAN:
            if(valueString.toLowerCase().equals("true"))
                return new BooleanValue(true);
            else if(valueString.toLowerCase().equals("false"))
                return new BooleanValue(false);
            else
                return null;
        case AUTO:
            // For performance it is check if it is a number before parsing instead of trying to parse and get the exception
            if(NUMERIC_PATTERN.matcher(valueString).find())
                return new FloatValue(Float.parseFloat(valueString));
            else if(valueString.toLowerCase().equals("true"))
                return new BooleanValue(true);
            else if(valueString.toLowerCase().equals("false"))
                return new BooleanValue(false);
            else
                return new StringValue(valueString);
        }
        
        throw new RuntimeException("an option of the enum was not covered");
    }

    public Column getColum() {
        switch (type) {
        case STRING:
            return struct(
                        lit(null).cast(DataTypes.DoubleType).as("num"),
                        col(key).cast(DataTypes.StringType).as("str"),
                        lit(null).cast(DataTypes.BooleanType).as("bool")
                   );
        case NUMERIC:
            return struct(
                        when(col(key).rlike(NUMERIC_PATTERN.pattern()), 
                                col(key))
                                .otherwise(lit(null))
                            .cast(DataTypes.DoubleType).as("num"),
                        lit(null).cast(DataTypes.StringType).as("str"),
                        lit(null).cast(DataTypes.BooleanType).as("bool")
                   );
        case BOOLEAN:
            return struct(
                        lit(null).cast(DataTypes.DoubleType).as("num"),
                        lit(null).cast(DataTypes.StringType).as("str"),
                        when(lower(col(key)).equalTo("true").or(lower(col(key)).equalTo("false")), 
                                col(key))
                                .otherwise(lit(null))
                            .cast(DataTypes.BooleanType).as("bool")
                   );
        default:
            return struct(
                        when(col(key).rlike(NUMERIC_PATTERN.pattern()), 
                                col(key))
                                .otherwise(lit(null))
                            .cast(DataTypes.DoubleType).as("num"),
                        col(key).cast(DataTypes.StringType).as("str"),
                        when(lower(col(key)).equalTo("true").or(lower(col(key)).equalTo("false")), 
                                col(key))
                                .otherwise(lit(null))
                            .cast(DataTypes.BooleanType).as("bool")
               );
        }
    }

    public Type getType() {
        return type;
    }

}
