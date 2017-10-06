package ch.cern.spark;

import java.io.Serializable;

import org.apache.spark.streaming.api.java.AbstractJavaDStreamLike;

public abstract class Stream<T extends AbstractJavaDStreamLike<?, ?, ?>> implements Serializable {
    
    private static final long serialVersionUID = 7586206631012207833L;
    
    private transient T stream;

    public Stream(T stream) {
        this.stream = stream;
    }

    public T stream(){
        return stream;
    }

}
