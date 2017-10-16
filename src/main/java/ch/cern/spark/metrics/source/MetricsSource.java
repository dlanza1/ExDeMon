package ch.cern.spark.metrics.source;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import ch.cern.Component;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.Metric;

public abstract class MetricsSource extends Component{

    private static final long serialVersionUID = -6197974524956447741L;
    
    public MetricsSource() {
        super(Type.METRIC_SOURCE);
    }
    
    public MetricsSource(Class<? extends Component> subClass, String name) {
        super(Type.METRIC_SOURCE, subClass, name);
    }

    public Stream<Metric> createStream(JavaStreamingContext ssc){
    		return Stream.from(createJavaDStream(ssc));
    }
    
    public abstract JavaDStream<Metric> createJavaDStream(JavaStreamingContext ssc);
    
}
