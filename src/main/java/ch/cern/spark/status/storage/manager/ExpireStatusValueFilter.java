package ch.cern.spark.status.storage.manager;

import java.time.Duration;
import java.time.Instant;

import org.apache.spark.api.java.function.Function;

import ch.cern.spark.status.StatusKey;
import ch.cern.spark.status.StatusValue;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import scala.Tuple2;

@ToString
@EqualsAndHashCode(callSuper=false)
public class ExpireStatusValueFilter implements Function<Tuple2<StatusKey, StatusValue>, Boolean> {

    private static final long serialVersionUID = 8935468162326857198L;
    
    private Instant oldest;

    public ExpireStatusValueFilter(Duration expired_period) {
        this.oldest = Instant.now().minus(expired_period);
    }

    @Override
    public Boolean call(Tuple2<StatusKey, StatusValue> tuple) throws Exception {
        return tuple._2 == null || tuple._2.getStatus_update_time() < oldest.toEpochMilli();
    }

}
