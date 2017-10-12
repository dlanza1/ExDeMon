package ch.cern.spark;

import java.io.Serializable;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaMapWithStateDStream;

import scala.Tuple2;

public abstract class MapWithStateStream<A, B, C, D> implements Serializable {
    
    private static final long serialVersionUID = 7586206631012207833L;
    
    private transient JavaMapWithStateDStream<A, B, C, D> stream;

    public MapWithStateStream(JavaMapWithStateDStream<A, B, C, D> stream) {
        this.stream = stream;
    }

    public JavaMapWithStateDStream<A, B, C, D> stream(){
        return stream;
    }
    
    public JavaDStream<Tuple2<A, C>> stateSnapshots() {
    		return stream.stateSnapshots().map(tuple -> tuple);
    }

}
