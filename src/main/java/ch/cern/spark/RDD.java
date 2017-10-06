package ch.cern.spark;

import java.io.Serializable;

import org.apache.spark.api.java.AbstractJavaRDDLike;

public abstract class RDD<T extends AbstractJavaRDDLike<?, ?>> implements Serializable {
    
    private static final long serialVersionUID = 7586206631012207833L;
    
    private transient T rdd;

    public RDD(T rdd) {
        this.rdd = rdd;
    }

    public T rdd(){
        return rdd;
    }

}
