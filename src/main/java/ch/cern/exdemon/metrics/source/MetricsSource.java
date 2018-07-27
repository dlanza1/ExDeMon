package ch.cern.exdemon.metrics.source;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import ch.cern.exdemon.components.Component;
import ch.cern.exdemon.components.Component.Type;
import ch.cern.exdemon.components.ComponentType;
import ch.cern.exdemon.components.ConfigurationResult;
import ch.cern.properties.Properties;

@ComponentType(Type.METRIC_SOURCE)
public abstract class MetricsSource extends Component {

    private static final long serialVersionUID = -6197974524956447741L;
    
    private int partitions;

	@Override
	public ConfigurationResult config(Properties properties) {
		properties.isTypeDefined();
		
		partitions = (int) properties.getLong("partitions", -1);
		
		return ConfigurationResult.SUCCESSFUL();
	}
	
	public JavaDStream<String> stream(JavaStreamingContext ssc){
	    JavaDStream<String> stream = createJavaDStream(ssc);
	    
	    return partitions > 0 ? stream.repartition(partitions) : stream;
	}

	/**
	 * Obtain metrics from external services as JSON strings.
	 * 
	 * @param ssc Spark context
	 * @return DStream of valid JSON object strings.
	 */
	protected abstract JavaDStream<String> createJavaDStream(JavaStreamingContext ssc);
    
}
