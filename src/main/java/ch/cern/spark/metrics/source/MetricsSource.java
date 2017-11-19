package ch.cern.spark.metrics.source;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.components.ComponentType;
import ch.cern.spark.Stream;
import ch.cern.spark.metrics.Metric;
import ch.cern.spark.metrics.filter.MetricsFilter;

@ComponentType(Type.METRIC_SOURCE)
public abstract class MetricsSource extends Component{

    private static final long serialVersionUID = -6197974524956447741L;
    
    private String id;

	private MetricsFilter filter;
	
	@Override
	public void config(Properties properties) throws ConfigurationException {
		properties.isTypeDefined();
		
		filter = MetricsFilter.build(properties.getSubset("filter"));
	}
    
    public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

    public final Stream<Metric> createStream(JavaStreamingContext ssc){
    		JavaDStream<Metric> metricStream = createJavaDStream(ssc);
    	
    		JavaDStream<Metric> filteredMetricStream = metricStream.filter(filter::test);
    		
    		return Stream.from(filteredMetricStream).map(metric -> {
										    				metric.addID("$source", getId());
										    				return metric;
										    			});
    }
	
	public abstract JavaDStream<Metric> createJavaDStream(JavaStreamingContext ssc);
    
}
