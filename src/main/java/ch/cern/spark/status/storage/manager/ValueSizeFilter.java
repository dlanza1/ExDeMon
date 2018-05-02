package ch.cern.spark.status.storage.manager;

import org.apache.spark.api.java.function.Function;

import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import ch.cern.spark.status.storage.StatusSerializer;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import scala.Tuple2;

@ToString
@EqualsAndHashCode(callSuper=false)
public class ValueSizeFilter implements Function<Tuple2<StatusKey, StatusValue>, Boolean> {

    private static final long serialVersionUID = -6806354277199482268L;
    
    private long min_value_size;

    private StatusSerializer serializer;

    public ValueSizeFilter(long min_value_size, StatusSerializer serializer) {
        this.min_value_size = min_value_size;
        this.serializer = serializer;
    }

    @Override
    public Boolean call(Tuple2<StatusKey, StatusValue> tuple) throws Exception {
        StatusValue value = tuple._2;
        
        if(value == null)
            return true;
        
        byte[] serialized = serializer.fromValue(value);
        
        if(serialized == null)
            return true;        
        
        return serialized.length >= min_value_size;
    }

}
