package ch.cern.exdemon.struct.schema;

import static org.apache.spark.sql.functions.*;

import java.io.Serializable;
import java.util.regex.Pattern;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.types.DataTypes;

import ch.cern.properties.ConfigurationException;
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
    
    public void config(Properties props) throws ConfigurationException {
        key = props.getProperty(KEY_PARAM);
        if(key == null)
            throw new ConfigurationException(KEY_PARAM + " must be configured");
        
        regex = props.getProperty(REGEX_PARAM);
        if(regex != null) {
            if(Pattern.compile(regex).matcher("").groupCount() != 1)
                throw new ConfigurationException("Regex expression must contain exactly 1 capture group from which value will be extracted");
        }
        
        String typeString = props.getProperty(TYPE_PARAM);
        if(typeString != null)
            type = Type.valueOf(typeString.toUpperCase());
        else
            type = Type.AUTO;
        
        props.confirmAllPropertiesUsed();
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
