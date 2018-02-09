package ch.cern.spark.metrics.source;

import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import ch.cern.components.Component;
import ch.cern.components.Component.Type;
import ch.cern.components.ComponentType;
import ch.cern.properties.ConfigurationException;
import ch.cern.properties.Properties;
import ch.cern.spark.metrics.schema.MetricSchema;
import lombok.Getter;
import lombok.Setter;

@ComponentType(Type.METRIC_SOURCE)
public abstract class MetricsSource extends Component {

    private static final long serialVersionUID = -6197974524956447741L;

    @Getter @Setter
	private MetricSchema schema;

	@Override
	public void config(Properties properties) throws ConfigurationException {
		properties.isTypeDefined();
		
		Properties schemaProps = properties.getSubset("schema");
		if(schemaProps.size() > 0) {
			schemaProps.setProperty("sources", getId());
			
			this.schema = new MetricSchema(getId()).tryConfig(schemaProps);
		}else {
			schema = null;
		}
	}

	/**
	 * Obtain metrics from external services as JSON strings.
	 * 
	 * @param ssc Spark context
	 * @return valid JSON object string.
	 */
	public abstract JavaDStream<String> createJavaDStream(JavaStreamingContext ssc);
    
}
