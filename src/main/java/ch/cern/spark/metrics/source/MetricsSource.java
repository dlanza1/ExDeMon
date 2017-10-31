package ch.cern.spark.metrics.source;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.Metric;

@ComponentType(Type.METRIC_SOURCE)
public abstract class MetricsSource extends Component{

    private static final long serialVersionUID = -6197974524956447741L;
    
    private String id;
    
    public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

    public Stream<Metric> createStream(JavaStreamingContext ssc){
    		return Stream.from(createJavaDStream(ssc)).map(metric -> {
										    				metric.addID("$source", getId());
										    				return metric;
										    			});
    }
	
	public abstract JavaDStream<Metric> createJavaDStream(JavaStreamingContext ssc);
    
}
