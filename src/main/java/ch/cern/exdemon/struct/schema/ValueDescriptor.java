package ch.cern.exdemon.struct.schema;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.regexp_extract;
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.when;

import java.io.Serializable;
import java.util.regex.Pattern;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.properties.Properties;
import lombok.Getter;
import lombok.ToString;

@ToString
public class ValueDescriptor implements Serializable{

    private static final long serialVersionUID = -1567807965329553585L;
    
    private static final Pattern NUMERIC_PATTERN = Pattern.compile("-?\\d+(\\.\\d+)?");

    @Getter
    private String id;

    @Getter
    private String key;
    public static final String KEY_PARAM = "key";
    
    private String regex;
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
        
        regex = props.getProperty(REGEX_PARAM);
        if(regex != null) {
            if(Pattern.compile(regex).matcher("").groupCount() != 1)
                confResult.withError(REGEX_PARAM, "regex expression must contain exactly 1 capture group from which value will be extracted");
        }
        
        String typeString = props.getProperty(TYPE_PARAM);
        if(typeString != null)
            type = Type.valueOf(typeString.toUpperCase());
        else
            type = Type.AUTO;
        
        return confResult.merge(null, props.warningsIfNotAllPropertiesUsed());
    }

    public Column getColum() {
    	Column column = col(key);
    	
    	if(regex != null)
    		column = regexp_extract(col(key), regex, 1);
    	
        switch (type) {
        case STRING:
            return struct(
                        lit(null).cast(DataTypes.DoubleType).as("num"),
                        column.cast(DataTypes.StringType).as("str"),
                        lit(null).cast(DataTypes.BooleanType).as("bool")
                   );
        case NUMERIC:
            return struct(
                        when(column.rlike(NUMERIC_PATTERN.pattern()), 
                        		column)
                                .otherwise(lit(null))
                            .cast(DataTypes.DoubleType).as("num"),
                        lit(null).cast(DataTypes.StringType).as("str"),
                        lit(null).cast(DataTypes.BooleanType).as("bool")
                   );
        case BOOLEAN:
            return struct(
                        lit(null).cast(DataTypes.DoubleType).as("num"),
                        lit(null).cast(DataTypes.StringType).as("str"),
                        when(lower(column).equalTo("true").or(lower(column).equalTo("false")), 
                        		column)
                                .otherwise(lit(null))
                            .cast(DataTypes.BooleanType).as("bool")
                   );
        default:
            return struct(
                        when(column.rlike(NUMERIC_PATTERN.pattern()), 
                        		column)
                                .otherwise(lit(null))
                            .cast(DataTypes.DoubleType).as("num"),
                            column.cast(DataTypes.StringType).as("str"),
                        when(lower(column).equalTo("true").or(lower(column).equalTo("false")), 
                        		column)
                                .otherwise(lit(null))
                            .cast(DataTypes.BooleanType).as("bool")
               );
        }
    }

    public Type getType() {
        return type;
    }

}
